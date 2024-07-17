const {DbNotificationPg} = require ('..')

test ('json', async () => {

	expect (new DbNotificationPg ({}).isJSON).toBe (false)
	expect (new DbNotificationPg ({payload: ''}).isJSON).toBe (false)
	expect (new DbNotificationPg ({payload: 'my_queue'}).isJSON).toBe (false)
	expect (new DbNotificationPg ({payload: '{$x}'}).isJSON).toBe (false)

	const n = new DbNotificationPg ({payload: '{"type":"users"}'})

	expect (n.isJSON).toBe (true)
	expect (n.json).toStrictEqual ({"type":"users"})

})