const Path = require ('path')
const {Application, ResourcePool} = require ('doix')
const {DbChannelPg, DbPoolPg} = require ('..')

const logger = {log: _ => {}}//new ConsoleLogger ()
const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}

const db = {
	connectionString: process.env.CONNECTION_STRING,
}

const app = new Application ({modules, logger, pools: {
	db: new DbPoolPg ({db, logger}),
	db2: new ResourcePool (),
}})

test ('bad', () => {

	expect (() => new DbChannelPg (app)).toThrow ()	
	expect (() => new DbChannelPg (app, {})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: 1})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: ''})).toThrow ()	

	const ch = new DbChannelPg (app, {name: 'hotline'})
	ch.router = {}

})