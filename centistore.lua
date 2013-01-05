-- this code is just experimental, not useful.

local S = require "syscall"
local util = require "syscall.util"

local oldassert = assert
local function assert(cond, s)
  return oldassert(cond, tostring(s))
end

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

local function submit(t, k, buf, cmd, f, data)
  if #t.avail == 0 then return false end
  local slot = table.remove(t.avail)
  local offset = t.block * bl(t, k)
  if not buf then buf = t.buf[slot] end
  t.info[slot] = {opcode = cmd, buf = buf, f = f, data = data}
  t.iocb[slot] = {opcode = cmd, data = slot, fildes = t.fd, buf = buf, nbytes = t.block, offset = offset, resfd = t.efd}
  local a = S.t.iocb_array{t.iocb[slot]} -- this allocates ptr, could pass one in
  local ret = assert(S.io_submit(t.ctx, a))
  assert(ret == 1)
  return true
end

local function set(t, k, v, f, data)
  return submit(t, k, v, "pwrite", f, data)
end

local function get(t, k, f, data)
  return submit(t, k, nil, "pread", f, data)
end

local function retr(t, v)
  local r = assert(t.epfd:epoll_wait()) -- blocking as default timeout is -1
  assert(#r == 1)
  assert(r[1].fd == t.efd:getfd(), "expect to get fd of eventfd file back")
  local e = util.eventfd_read(t.efd)
  local r = assert(S.io_getevents(t.ctx, e, e))
  
  for i = 1, #r do
    assert(tonumber(r[i].res) == t.block, "expect full read/write got " .. tonumber(r[i].res))
    local slot = tonumber(r[i].data)
    local info = t.info[slot]
    if info.f then info.f(info.data, info.buf, info, r[i]) end
    t.avail[#t.avail + 1] = slot
  end
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

  local buf = {}
  for i = 0, queue - 1 do 
    buf[i] = assert(S.mmap(nil, block, "read, write", "private, anonymous", -1, 0))
  end

  if inet6 and not addr then addr = "::1" end
  if not inet6 and not addr then addr = "127.0.0.1" end

  local flags = "rdwr,creat"
  if direct then flags = flags .. ",direct" end

  local fd = assert(S.open(file, flags, "rusr,wusr"))

  local ok = assert(S.ftruncate(fd, size))

  local ok = assert(S.fadvise(fd, "random"))

  local family, sa = "inet", S.t.sockaddr_in
  if inet6 then family, sa = "inet6", S.t.sockaddr_in6 end
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

  local avail, iocb = {}, {}
  for i = 0, queue - 1 do
    avail[#avail + 1] = i
    iocb[#iocb + 1] = S.t.iocb()
  end

  return setmetatable({fd = fd, block = block, blocks = blocks, sock = sock, iocb = iocb,
    buf = buf, queue = queue, ctx = ctx, efd = efd, epfd = epfd, avail = avail, info = {}},
    {__index = {
      set = set,
      get = get,
      retr = retr,
      nnv = nnv,
      cast = function(t, b) return ffi.cast(nvp, b) end,
      copy = function(t, a, b) ffi.copy(a, b, b.n + ffi.offsetof(nv, "v")) end,
      full = function(t) return #t.avail == 0 end,
      time = function(t) return S.gettimeofday().time end
    }})
end

return setmetatable(cent, {
  __call = function(t, p) return cent.init(p) end,
})

