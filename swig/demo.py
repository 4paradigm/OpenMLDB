import rtidb 
nsc = rtidb.RTIDBClient("172.27.128.37:6183", "/issue-5")
print("begin put============================")
data = {"card":"card2","mcc":"mcc0", "p_biz_date":3}
nsc.put("test1", data, None)
print("begin query============================")
ro = rtidb.ReadOption()
ro.index.update({"card":"card0"})
resp = nsc.query("test1", ro)
print(resp)
for l in resp:
  print(l, resp[l])


print("begin update============================")
cond = {"card":"card2"}
v = {"mcc":"mcc1", "p_biz_date":4}
nsc.update("test1", cond, v, None)
