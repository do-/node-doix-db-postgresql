const {parse} = require ('pg-connection-string')
const {DbPoolPg} = require ('..')

const connectionString = process.env.CONNECTION_STRING, o = {db: {connectionString}}

test ('isSameDbAs', async () => {

	const somePool = new DbPoolPg (o)

	const similarPool = new DbPoolPg (o)

	const anotherPool = new DbPoolPg ({db: parse (connectionString)})

	const alienPool = new DbPoolPg ({db: {host: '0.0.0.0', port: 0}})
	
	expect (somePool.isSameDbAs ()).toBe (false)
	expect (somePool.isSameDbAs ({})).toBe (false)

	expect (somePool.isSameDbAs (somePool)).toBe (true)
	expect (somePool.isSameDbAs (similarPool)).toBe (true)
	expect (somePool.isSameDbAs (anotherPool)).toBe (true)
	expect (somePool.isSameDbAs (alienPool)).toBe (false)

})