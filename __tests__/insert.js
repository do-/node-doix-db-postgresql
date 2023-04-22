const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')
const {Readable} = require ('stream')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

const r = () => ['root1'].map (i => Path.join (__dirname, 'data', i))

const dir = {
	root: r (),
	live: false,
}

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const m = new DbModel ({dir, db: pool})

		m.loadModules ()
		
		await db.do ('DROP TABLE IF EXISTS tb_3')
		await db.do ('CREATE TABLE tb_3 (id int PRIMARY KEY, label text)')

		await db.insert ('tb_3', {id: 1, label: 'user'})
		expect (await db.getArray ('SELECT * FROM tb_3')).toStrictEqual ([{id: 1, label: 'user'}])

		await expect (db.insert ('tb_3', {id: 1, label: 'admin'})).rejects.toThrow ()

		await db.insert ('tb_3', {id: 1, label: 'admin'}, {onlyIfMissing: true})
		expect (await db.getArray ('SELECT * FROM tb_3')).toStrictEqual ([{id: 1, label: 'user'}])

	}
	
	finally {

		await db.release ()

	}

})