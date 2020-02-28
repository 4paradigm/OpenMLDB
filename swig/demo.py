import rtidb 
nsc = rtidb.RTIDBClient("172.27.128.37:6183", "/issue-5")

data = {"card":"card0","mcc":"mcc0", "p_biz_date":3}
nsc.put("test1", data, None)
ro = rtidb.ReadOption()
ro.index.update({"card":"card0"})
resp = nsc.query("test1", ro)
print(resp)
for l in resp:
  print(l, resp[l])
