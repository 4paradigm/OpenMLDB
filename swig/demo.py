import client
endpoint=""
msg=""
nsc = client.InitNsClient("172.27.128.33:6181", "/multidc2", endpoint)
print(dir(nsc))
table=""
resp = client.ShowTable(nsc, table)
for i in resp:
    print(i)
