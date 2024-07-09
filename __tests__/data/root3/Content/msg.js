module.exports = {

    do_send_msg: async function () {
    
		  const {db, rq: {id}} = this

      await db.do ('DELETE FROM doix_test_db_4.tb_1 WHERE id = ?', [id])

      return id

    },
    
}