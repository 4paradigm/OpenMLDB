import client
nsc = client.RtidbNSClient("172.27.128.33:6181", "/multidc2", "")
resp = nsc.ShowTable("")
for i in resp:
    print(i)
