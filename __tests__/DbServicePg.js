const {DbServicePg} = require ('..')

test ('test', async () => {

	const svc = new DbServicePg ()

	expect (() => svc.test ()).toThrow ()
	expect (() => svc.test (1)).toThrow ()
	expect (() => svc.test (null)).toThrow ()
	expect (() => svc.test ({})).toThrow ()
	expect (() => svc.test ({payload: 1})).toThrow ()
	expect (() => svc.test ({payload: ''})).toThrow ()

	expect (svc.test ({payload: 'some_queue'})).toBe (false)
	expect (svc.test ({payload: '{'})).toBe (false)
	expect (svc.test ({payload: '{]'})).toBe (false)
	expect (svc.test ({payload: '{"type":"users"}'})).toBe (true)

})