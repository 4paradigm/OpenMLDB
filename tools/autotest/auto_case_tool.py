import random
import string

def getRandomName(prefix="auto_", len=8):
    tableName = ''.join(random.sample(string.ascii_letters + string.digits, len))
    tableName = prefix+tableName
    return tableName