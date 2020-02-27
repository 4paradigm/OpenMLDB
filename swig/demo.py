import client
nsc = client.RtidbNSClient()
ok = nsc.Init("172.27.128.33:6182", "/multidc2", "")
if not ok:
  print("initial ns client failed!")

ok = nsc.Init("172.27.128.33:6181", "/multidc2", "")
if ok:
  print("initial ns client success!")

resp = nsc.ShowTable("")
for i in resp:
    print(i)

tc = client.RtidbTabletClient();
ok = tc.Init("172.27.128.31:3001")
if ok:
  print("initial tablet client success!")
else:
  print("intial tablet client failed!")
  exit(-1)

ok = tc.Put(7, 0, "rtidb", 2, "develop")
if ok:
  print("tablet client put success!")
else:
  print("tablet put failed!")
  exit(-1)
value = tc.Get(7, 0, "rtidb", 0)
print(value)
