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

local keys = {
  "one", "two", "three", "four", "five", "test", "best", "rest",
  "one1", "two1", "three1", "four1", "five1", "test1", "best1", "rest1",
  "one2", "two2", "three2", "four2", "five2", "test2", "best2", "rest2",
  "one3", "two3", "three3", "four3", "five3", "test3", "best3", "rest3",
  "one4", "two4", "three4", "four4", "five4", "test4", "best4", "rest4",
  "one5", "two5", "three5", "four5", "five5", "test5", "best5", "rest5",
  "one6", "two6", "three6", "four6", "five6", "test6", "best6", "rest6",
  "one7", "two7", "three7", "four7", "five7", "test7", "best7", "rest7",
}

for i = 1, #keys do
  if c:full() then c:retr() end
  assert(c:set(keys[i], banana))
end

c:retr()

local count = 100000
local tv0 = c:time()

for i = 1, count do
  if c:full() then c:retr() end
  assert(c:get(keys[(i % #keys) + 1], get, v))
end

local tv1 = c:time()

print("iops: ", math.floor(count / (tv1 - tv0)))

