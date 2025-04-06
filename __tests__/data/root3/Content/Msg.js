module.exports = {

    doSend: async function () {
    
		  const {db, request: {id}} = this

      await db.do ('DELETE FROM doix_test_db_4.tb_1 WHERE id = ?', [id])

      return id

    },
    
}