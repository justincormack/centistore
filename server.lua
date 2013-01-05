local s = require "centistore"

local c, err = s.init{direct = true} -- init server

if not c then error(err) end

local banana = c:nnv("banana")
local v = c:nnv("")

local function set() end -- nothing to do here (ack client later)
local function get(v, buf)
  local vv = c:cast(buf)
  c:copy(v, vv)
  --print(tostring(v))
end

for i = 1, 1000 do
  assert(c:set("test", banana))
  c:retr()
  assert(c:get("test", get, v))
  c:retr()
end

print(tostring(v))

