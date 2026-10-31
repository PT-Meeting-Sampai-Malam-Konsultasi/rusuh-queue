#!lua name=rusuh_q
--
-- Valkey function library for the queue service (Solution Architecture: "Scripts").
-- Loaded at start with FUNCTION LOAD REPLACE. REPLACE swaps the whole library, so
-- while a rolling deploy runs old and new tasks side by side, keep the previous
-- version of a function (rusuh_x_v1) next to its successor (rusuh_x_v2) until no
-- old task is left. Every key a function touches is passed in KEYS.

-- rusuh_ratelimit_v1: fixed-window counter.
-- KEYS[1] = counter key; ARGV[1] = window in milliseconds.
-- Returns {count in this window, milliseconds until the window resets}.
local function ratelimit_v1(keys, args)
  local window = tonumber(args[1])
  local n = server.call('INCR', keys[1])
  if n == 1 then
    server.call('PEXPIRE', keys[1], window)
    return {n, window}
  end
  local ttl = server.call('PTTL', keys[1])
  if ttl < 0 then
    -- a counter without expiry would block forever; repair it
    server.call('PEXPIRE', keys[1], window)
    ttl = window
  end
  return {n, ttl}
end

server.register_function('rusuh_ratelimit_v1', ratelimit_v1)
