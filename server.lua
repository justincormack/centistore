local s = require "centistore"

local c, err = s.init{direct = true} -- init server

if not c then error(err) end

local banana = c:nnv("banana")
local v = c:nnv("")

for i = 1, 1000 do
  c:set("test", banana)
  local v = c:get("test", v)
end

print(tostring(v))

