const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

const src = Path.join (__dirname, 'data', 'root1')

afterAll(async () => {

	await pool.pool.end ()

})

test ('basic', async () => {

	const m = new DbModel ({src, db: pool})
	m.loadModules ()

	try {

		var db = await pool.toSet (job, 'db')

		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.createTempTable ('tb_2')
		await db.do ('ALTER TABLE tb_2 ADD PRIMARY KEY (id)')

		expect (db.txn).toBeNull ()

		await db.commit ()		
		await db.rollback ()		

		await db.begin ()
		expect (db.txn).toStrictEqual ({})

		await db.insert ('tb_2', {id: 1, label: 'user'})
		expect (db.txn).toStrictEqual ({})

		await db.commit ()		
		expect (db.txn).toBeNull ()
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'user'}])

		await db.begin ()
		expect (db.txn).toStrictEqual ({})

		await db.update ('tb_2', {id: 1, label: 'admin'})
		expect (db.txn).toStrictEqual ({})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'admin'}])

		await db.rollback ()		
		expect (db.txn).toBeNull ()
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'user'}])

		await db.begin ()

	}	
	finally {

		expect (db.txn).toStrictEqual ({})
		await db.release ()
		expect (db.txn).toBeNull ()

	}

})