const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')

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
		
		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.do ('CREATE TABLE tb_2 (id int, label text)')
		await db.do ('INSERT INTO tb_2 (id, label) VALUES (?, ?)', [1, 'user'])
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'user'}])

		await db.update ('tb_2', {id: 1, label: 'admin'})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'admin'}])

		await db.update ('tb_2', {id: 1})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'admin'}])

	}
	finally {

		await db.release ()

	}

})