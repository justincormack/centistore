-- this code is just experimental, not useful.

local S = require "syscall"
local util = require "syscall.util"

local oldassert = assert
local function assert(cond, s)
  return oldassert(cond, tostring(s))
end

local t = S.t

local ffi = require "ffi"

local murmer = ffi.load("murmer.so.1")

ffi.cdef [[
uint32_t PMurHash32(uint32_t seed, const void *key, int len);
]]

ffi.cdef [[
struct nv {
  int32_t n;
  char v[?];
};
]]

local cent = {}

local char = ffi.typeof("char *")
local int32 = ffi.typeof("int32_t *")

local nv = ffi.metatype("struct nv", {
  __len = function(t) return t.n end,
  __tostring = function(t) return ffi.string(t.v, t.n) end,
})

local nvp = ffi.typeof("struct nv *")

local nnv = function(t, a, b, mem)
  if type(a) == "string" then b = #a end
  mem = mem or assert(S.mmap(nil, t.block, "read, write", "private, anonymous", -1, 0))
  local tp = ffi.cast(nvp, mem)
  tp[0].n = b
  ffi.copy(tp[0].v, a, b)
  return tp[0]
end

local function sv(k) -- return size, value
  if type(k) == "string" then return #k, k end
  return k.n, k.v
end

local function bl(t, k)
  local s, v = sv(k)
  local hash = murmer.PMurHash32(0, v, s)
  return hash % t.blocks
end

local function set(t, k, v)
  if type(v) == "string" then v = nnv(t, v, #v, t.buf) end
  local n = v.n
  local offset = t.block * bl(t, k)
  --local ret = assert(S.pwrite(t.fd, v, t.block, offset))
  --assert(ret == t.block, "no short write: ")
  local ret = assert(S.io_submit(t.ctx, {{cmd = "pwrite", data = 0, fd = t.fd, buf = S.pt.void(v), nbytes = t.block, offset = offset, resfd = t.efd}}))
  assert(ret == 1)
  local r = assert(t.epfd:epoll_wait()) -- blocking as default timeout is -1
  assert(#r == 1)
  assert(r[1].fd == t.efd:getfd(), "expect to get fd of eventfd file back")
  local e = util.eventfd_read(t.efd)
  assert(e == 1, "expect to be told one aio event ready")
  local r = assert(S.io_getevents(t.ctx, e, e))
  assert(#r == 1, "expect one aio event")
  assert(r[1].data == 0, "expect to get data back")
  assert(r[1].res == t.block, "expect full write")
end

local function get(t, k, v)
  local offset = t.block * bl(t, k)
  --local ret = assert(S.pread(t.fd, t.buf, t.block, offset))
  --assert(ret == t.block, "no short read")
  -- TODO don't allocate iocb all the time. note must not be modified, so allocate set of them
  -- TODO data will store queue point
  local ret = assert(S.io_submit(t.ctx, {{cmd = "pread", data = 0, fd = t.fd, buf = t.buf, nbytes = t.block, offset = offset, resfd = t.efd}}))
  assert(ret == 1)
  local r = assert(t.epfd:epoll_wait()) -- blocking as default timeout is -1
  assert(#r == 1)
  assert(r[1].fd == t.efd:getfd(), "expect to get fd of eventfd file back")
  local e = util.eventfd_read(t.efd)
  assert(e == 1, "expect to be told one aio event ready")
  local r = assert(S.io_getevents(t.ctx, e, e))
  assert(#r == 1, "expect one aio event")
  assert(r[1].data == 0, "expect to get data back")
  assert(r[1].res == t.block, "expect full read")
  local vv = ffi.cast(nvp, t.buf)[0]
  local v = v or nv(vv.n)
  ffi.copy(v, vv, vv.n + 4)
  return v
end

function cent.init(param)
  param = param or {}
  local file = param.file or "./cs.dat"
  local direct = true
  if param.direct == false then direct = false end
  local block = param.block or 4096
  local tcp = param.tcp or false
  local port = param.port or 5999
  local inet6 = param.inet6 or false
  local addr = param.addr
  local blocks = param.blocks or 4096 -- must be multiple of 512 for direct io
  local size = blocks * block
  local queue = param.queue or 32
  local buf = assert(S.mmap(nil, block * queue, "read, write", "private, anonymous", -1, 0))

  if inet6 and not addr then addr = "::1" end
  if not inet6 and not addr then addr = "127.0.0.1" end

  local flags = "rdwr,creat"
  if direct then flags = flags .. ",direct" end

  local fd = assert(S.open(file, flags, "rusr,wusr"))

  local ok = assert(S.ftruncate(fd, size))

  local ok = assert(S.fadvise(fd, "random"))

  local family, sa = "inet", t.sockaddr_in
  if inet6 then family, sa = "inet6", t.sockaddr_in6 end
  local socktype = "dgram"
  if tcp then socktype = "stream" end

  local sock = assert(S.socket(family, socktype .. ",nonblock"))

  local sa = sa(port, addr)
  local ok = assert(sock:bind(sa))

  if tcp then
    local ok = assert(sock:listen())
  end

  local ctx = assert(S.io_setup(queue))
  local efd = assert(S.eventfd(0, "nonblock"))
  local epfd = assert(S.epoll_create())
  assert(epfd:epoll_ctl("add", efd, "in"))

  return setmetatable({fd = fd, block = block, blocks = blocks, sock = sock,
    buf = buf, queue = queue, ctx = ctx, efd = efd, epfd = epfd, q0 = 0, q1 = 0},
    {__index = {
      set = set,
      get = get,
      nnv = nnv
    }})
end

return setmetatable(cent, {
  __call = function(t, p) return cent.init(p) end,
})

