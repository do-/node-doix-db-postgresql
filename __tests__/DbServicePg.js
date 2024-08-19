const Path = require ('path')
const {DbServicePg, DbNotificationPg, DbPoolPg} = require ('..')
const {Application} = require ('doix')
const {Router: {TEST_MESSAGE}} = require ('protocol-agnostic-router')

const {Writable} = require ('stream')
const winston = require ('winston')
const logger = winston.createLogger({
	transports: [
//	  new winston.transports.Console (),
	  new winston.transports.Stream ({stream: new Writable ({write(){}})})
	],
	format: winston.format.combine (
		winston.format.timestamp ({format: 'YYYY-MM-DD[T]hh:mm:ss.SSS'}),
		winston.format.printf ((i => `${i.timestamp} ${i.event} ${i.id} ${i.event === 'finish' ? i.elapsed + ' ms' : i.message}${i.details ? ' ' + JSON.stringify (i.details) : ''}`))
	),
})

const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}
	
const db = {
	connectionString: process.env.CONNECTION_STRING,
}
	
const app = new Application ({modules, logger, pools: {db: new DbPoolPg ({db, logger})}})

test ('test', async () => {

	const svc = new DbServicePg (app, {name: '1'})

	expect (svc [TEST_MESSAGE] (new DbNotificationPg({payload: 'some_queue'}))).toBe (false)
	expect (svc [TEST_MESSAGE] (new DbNotificationPg({payload: '{'}))).toBe (false)
	expect (svc [TEST_MESSAGE] (new DbNotificationPg({payload: '{]'}))).toBe (false)
	expect (svc [TEST_MESSAGE] (new DbNotificationPg({payload: '{"type":"users"}'}))).toBe (true)

})