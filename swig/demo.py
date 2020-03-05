import rtidb 
try:
  nsc = rtidb.RTIDBClient("172.27.128.37:618", "/issue-5")
except Exception as e:
  print(e)
nsc = rtidb.RTIDBClient("172.27.128.37:6181", "/issue-5")
  

print("begin put============================")
data = {"card":"card3","mcc":"mcc3", "p_biz_date":3}
nsc.put("test1", data, None)
print("begin query============================")
ro = rtidb.ReadOption()
ro.index.update({"card":"card3"})
resp = nsc.query("test1", ro)
print("size ", resp.count())
for l in resp:
  for k in l:
    print(k, l[k])


#print("begin update============================")
#cond = {"card":"card2"}
#v = {"mcc":"mcc1", "p_biz_date":4}
#nsc.update("test1", cond, v, None)
#
#resp = nsc.batch_query("test1", [ro])
#print("size ", resp.count())
#for l in resp:
#  print(l)

resp = nsc.traverse("test1")
print("size ", resp.count())
for l in resp:
  print(l)
