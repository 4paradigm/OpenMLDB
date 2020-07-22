# list的设计

## ListRef

```c
struct ListRef {
	int_8* list
}

struct IteratorRef {
	int_8* iterator;
}
```

### 支持At操作
```python
def at_test(col:list<int>, pos:int):int
	return col[pos]
end
```

