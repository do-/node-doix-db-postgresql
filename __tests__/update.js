const Path = require ('path')
const {DbModel} = require ('doix-db')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')
const {Readable} = require ('stream')

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

		var db = await pool.setResource (job, 'db')

		await db.do ('DROP TABLE IF EXISTS tb_2')
		await db.createTempTable ('tb_2')
		await db.do ('ALTER TABLE tb_2 ADD PRIMARY KEY (id)')

		await db.insert ('tb_2', {id: 1, label: 'user'})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'user'}])

		await db.update ('tb_2', {id: 1, label: 'admin'})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'admin'}])

		await db.update ('tb_2', {id: 1})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'admin'}])

		await db.upsert ('tb_2', {id: 1, label: 'user'}, {key: ['id']})
		expect (await db.getArray ('SELECT * FROM tb_2')).toStrictEqual ([{id: 1, label: 'user'}])

		await db.do ('TRUNCATE tb_2')
		
		await expect (db.putBinaryStream ('tb_-1')).rejects.toThrow ()

		{
		
			const os = await db.putBinaryStream ('tb_2', ['id', 'label'])
			await new Promise ((ok, fail) => {
				os.on ('error', fail)
				os.on ('complete', ok)
				Readable.from ([
					'1\tadmin\n',
					'2\tuser\n',
				]).pipe (os)
			})

			expect (await db.getArray ('SELECT * FROM tb_2 ORDER BY id')).toStrictEqual ([
				{id: 1, label: 'admin'},
				{id: 2, label: 'user'},
			])

		}

		{

			const os = await db.putBinaryStream ('tb_2', ['id', 'label'])

			let err

			await expect (new Promise ((ok, fail) => {
				os.on ('error', _ => err = _)
				os.on ('error', fail)
				os.on ('finish', ok)
				Readable.from (['zzz']).pipe (os)
			})).rejects.toThrow ()

			expect (err.stack).toMatch ('update.js')

		}

	}
	
	finally {

		await db.release ()

	}

})