module.exports = {

    comment: 'Queue 1',

    columns: {
        id       : 'int    // PK',
    },
    
    pk: 'id',

    queue: {
        request: {type: 'msg', action: 'send'},
        order: 'id',
    },

    sql: `SELECT id FROM doix_test_db_4.tb_1`,

}