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

test ('model', async () => {

	try {

		var db = await pool.toSet (job, 'db')

		const model = new DbModel ({dir, db: pool})

		expect ([...db.lang.genDDL (model)]).toHaveLength (0)

		model.loadModules ()

		for (const [sql] of db.lang.genDDL (model)) await db.do (sql)
		
		const a = await db.getArray ('SELECT * FROM vw_1')

		expect (a).toStrictEqual ([{id: 1}])

	}
	finally {

		await db.release ()

	}

})