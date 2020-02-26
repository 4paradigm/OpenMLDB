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
