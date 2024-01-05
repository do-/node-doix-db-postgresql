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

afterAll(async () => {

	await pool.pool.end ()

})

test ('model', async () => {

	try {

		const schemaName = 'doix_test_db_3'

		const model = new DbModel ({
			src: {
				schemaName,
				root: Path.join (__dirname, 'data', 'root1')
			},
			db: pool
		})

		var db = await pool.toSet (job, 'db')

		await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)

		expect (model.lang.getTypeDefinition ('xls').name).toBe ('XLS') // sic

		model.loadModules ()
		
		const plan = db.createMigrationPlan ()

		await plan.loadStructure ()
		plan.inspectStructure ()

		await db.doAll (plan.genDDL ())

		await db.do (`SET SCHEMA '${schemaName}'`)

		{
		
			await db.do ('CALL proc_1 (0)')
			
			{
				
				const REF = Math.floor (100 * Math.random ())
			
				await expect (db.do ('INSERT INTO tb_3 (id) VALUES (?)', [REF])).rejects.toThrow ()
				await db.do ('INSERT INTO tb_2 (id) VALUES (?)', [REF])
				await db.do ('INSERT INTO tb_3 (id) VALUES (?)', [REF])

				const c0 = await db.getScalar ('SELECT COUNT(*) FROM tb_3')
				expect (parseInt (c0)).toBe (1)

				await db.do ('DELETE FROM tb_2 WHERE id = ?', [REF])
				const c1 = await db.getScalar ('SELECT COUNT(*) FROM tb_3')
				expect (parseInt (c1)).toBe (0)

			}
			
			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.00', cnt: 1},
					{id: 1, label: 'on', amount: '0.00', cnt: 1},
				])
			
			}

			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label', [1, 'one'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.00', cnt: 1},
					{id: 1, label: 'one', amount: '0.00', cnt: 2},
				])
			
			}

			try {

				await db.do ('INSERT INTO tb_1 (id, label, cnt) VALUES (?, ?, ?)', [-1, 'one', 10])

			}
			catch (err) {

				expect (err.message).toBe (err.code)

			}			

			try {

				await db.begin ()				
				await db.do ('INSERT INTO tb_1 (id, label, cnt) VALUES (?, ?, ?)', [-1, 'zzz', -1])
				await db.do ('INSERT INTO tb_1 (id, label, cnt) VALUES (?, ?, ?)', [-2, 'zzz', -1])

			}
			catch (err) {

				// do nothing

			}			
			finally {

				db.rollback ()

			}

			try {

				await db.begin ()

				await db.do ('CREATE UNIQUE INDEX ___label ON tb_2 (label)')

				await db.do ('INSERT INTO tb_2 (id, label) VALUES (?, ?)', [-1, 'zzz'])
				await db.do ('INSERT INTO tb_2 (id, label) VALUES (?, ?)', [-2, 'zzz'])

			}
			catch (err) {

				// do nothing

			}			
			finally {

				db.rollback ()

			}

		}
		
		{

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			expect ([...plan.toDo.keys ()].sort ()).toStrictEqual ([
				'comment', 
				'recreate'
			])

			plan.toBe.get ('tb_1').keys.amount = null

			let drops = 0; for (const [sql, params] of plan.genDDL ()) {

				if (/^DROP INDEX/.test (sql)) drops ++

				await db.do (sql, params)

			} 

			expect (drops).toBe (1)

		}

		{
		
			await db.do (`ALTER TABLE tb_1 DROP COLUMN amount CASCADE`)

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', cnt: 2},
					{id: 1, label: 'one', cnt: 2},
				])
			
			}

			for (const [sql, params] of plan.genDDL ()) await db.do (sql, params)

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.00', cnt: 2},
					{id: 1, label: 'one', amount: '0.00', cnt: 2},
				])
			
			}

		}
		
		{
		
			await db.do (`ALTER TABLE tb_1 ALTER amount DROP NOT NULL, ALTER amount SET DEFAULT 1, ALTER amount TYPE DECIMAL(5,1), ALTER label SET NOT NULL, ALTER id SET DEFAULT 0`)

			await db.do ('INSERT INTO tb_1 (id, label) VALUES (?, ?)', [2, 'two'])

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.0', cnt: 2},
					{id: 1, label: 'one', amount: '0.0', cnt: 2},
					{id: 2, label: 'two', amount: '1.0', cnt: 1}
				])
			
			}

			await db.do ('UPDATE tb_1 SET amount = NULL WHERE id = ?', [2])

			const plan = db.createMigrationPlan (); await plan.loadStructure (); plan.inspectStructure ()

			await db.doAll (plan.genDDL ())

			{

				const a = await db.getArray ('SELECT * FROM tb_1 ORDER BY id')

				expect (a).toStrictEqual ([
					{id: 0, label: 'zero', amount: '0.00', cnt: 2},
					{id: 1, label: 'one', amount: '0.00', cnt: 2},
					{id: 2, label: 'two', amount: '0.00', cnt: 2}
				])
			
			}

		}

		{

			const a = await db.getArray ('SELECT * FROM vw_1 ORDER BY id')

			expect (a).toStrictEqual ([{id: 1}])

		}

		{

			const o = await db.getObject ('vw_1', [1])

			expect (o).toStrictEqual ({id: 1})
		}
		
		{

			const o = await db.getObject ('vw_1', [1])

			expect (o).toStrictEqual ({id: 1})

		}

		{

			const o = await db.getObject ('vw_1', [2], {notFound: {}})

			expect (o).toStrictEqual ({})

		}

		const q = model.createQuery ([
			['vw_1', {
				columns: ['id'],
				filters: [['id', '=', 1]],
			}],
		])		

		const id = await db.getScalar (q)

		expect (id).toBe (1)

		const is = await db.getStream (q)
		
		for await (const r of is) expect (r).toStrictEqual ({id: 1})
		
		{

			const qc = model.createQuery ([['vw_1']], {
				order: ['id'],
				limit: 1,
				offset: 0,
			})		

			const l = await db.getArray (qc)

			expect (l).toStrictEqual ([{id: 1}])
			expect (l [Symbol.for ('count')]).toBe (1)
			expect (l [Symbol.for ('query')]).toBe (qc)
		
		}

		await db.do (`DROP SCHEMA ${schemaName} CASCADE`)

	}
	finally {

		await db.release ()

	}

})