const {DbServicePg, DbNotificationPg} = require ('..')

test ('test', async () => {

	const svc = new DbServicePg ()

	expect (svc.test (new DbNotificationPg({payload: 'some_queue'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{]'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{"type":"users"}'}))).toBe (true)

})