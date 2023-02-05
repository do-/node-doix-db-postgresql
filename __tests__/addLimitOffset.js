const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('LIMIT 1', () => {

	const qp = ['SELECT * FROM users']
	
	lang.addLimitOffset (qp, 1)

	expect (qp).toStrictEqual ([1, 0, 'SELECT * FROM users LIMIT ? OFFSET ?'])

})