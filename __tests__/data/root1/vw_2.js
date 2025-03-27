module.exports = {

    comment: 'View 2',

    columns: {
        id       : 'int    // PK',
    },
    
    pk: 'id',

    wrap: true,

    sql: `SELECT 'two' AS label, 2 AS id`,

}