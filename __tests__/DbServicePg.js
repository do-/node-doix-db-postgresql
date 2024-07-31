const Path = require ('path')
const {DbServicePg, DbNotificationPg, DbPoolPg} = require ('..')
const {Application} = require ('doix')

const logger = 
{log: _ => {}}
//new ConsoleLogger ()
const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}
	
const db = {
	connectionString: process.env.CONNECTION_STRING,
}
	
const app = new Application ({modules, logger, pools: {db: new DbPoolPg ({db, logger})}})

test ('test', async () => {

	const svc = new DbServicePg (app)

	expect (svc.test (new DbNotificationPg({payload: 'some_queue'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{]'}))).toBe (false)
	expect (svc.test (new DbNotificationPg({payload: '{"type":"users"}'}))).toBe (true)

})