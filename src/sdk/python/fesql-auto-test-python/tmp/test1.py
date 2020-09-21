import re
names = ["int 222 int32"]
name = names[0]
ss = re.split("\\s+",name)
type = ss[-1]
if type=="int32":
    rtype = "int"
name = name[0:-len(type)]+rtype
print(name)
print(names)