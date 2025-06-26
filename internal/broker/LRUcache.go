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

func (c *Cache) Get(key string) (interface{}, bool) {
	if elem, found := c.items[key]; found {
		c.list.MoveToFront(elem)

		return elem.Value.(*entry).value, true
	}

	return nil, false
}

func (c *Cache) Put(key string, value *PartitionWriter) {
	if elem, ok := c.items[key]; ok {
		c.list.MoveToFront(elem)
		elem.Value.(*entry).value = value
		return
	}

	if c.list.Len() >= c.capacity {
		back := c.list.Back()
		if back != nil {

			write := back.Value.(*entry).value
			write.Close() // Close the PartitionWriter before removing it
			// Remove the least recently used item
			c.list.Remove(back)
			delete(c.items, back.Value.(*entry).key)
		}
	}

	e := &entry{key: key, value: value}
	elem := c.list.PushFront(e)
	c.items[key] = elem
}

func (c *Cache) Len() int {
	return c.list.Len()
}
