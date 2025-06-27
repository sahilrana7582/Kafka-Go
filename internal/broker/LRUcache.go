package broker

import "container/list"

type entry struct {
	key   string
	value *PartitionWriter
}

type Cache struct {
	capacity int
	items    map[string]*list.Element
	list     *list.List
}

func New(cap int) *Cache {
	return &Cache{
		capacity: cap,
		items:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *Cache) Get(key string) (*PartitionWriter, bool) {
	if elem, found := c.items[key]; found {
		c.list.MoveToFront(elem)
		return elem.Value.(*entry).value, true
	}

	return nil, false
}

func (c *Cache) Put(key string, value *PartitionWriter) {
	if _, ok := c.items[key]; ok {
		return
	}

	e := &entry{key: key, value: value}
	elem := c.list.PushFront(e)
	c.items[key] = elem
}

func (c *Cache) Len() int {
	return c.list.Len()
}
