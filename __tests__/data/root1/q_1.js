module.exports = {

    comment: 'Queue 1',

    columns: {
        id       : 'int    // PK',
    },
    
    pk: 'id',

    queue: {
        rq:    {type: 'msg', action: 'send'},
        order: 'id',
    },

    sql: `SELECT UNNEST (ARRAY [2, 1, 3]) id`,

}