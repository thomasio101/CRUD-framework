import CRUDImplementation from './CRUDImplementation';
import * as mysql from 'mysql2/promise';

type AtLeastOne<T, U = {[K in keyof T]: Pick<T, K> }> = Partial<T> & U[keyof U]

class MySQLCRUD<T, identifierColumn extends keyof T> implements CRUDImplementation<T, number, MySQLCRUD.CreateCommand<T, identifierColumn>, MySQLCRUD.UpdateCommand<T, identifierColumn>, MySQLCRUD.DeleteCommand<T, identifierColumn>> {
    constructor(private connectionPool: mysql.Pool, private table: string, private identifierColumn: string) { };

    async get(identifer: number): Promise<T> {
        const connection = await this.connectionPool.getConnection()

        const [results] = await connection.query(
            'SELECT * FROM ?? WHERE ?? = ? LIMIT 1;',
            [this.table, this.identifierColumn, identifer]
        ) as [T[], mysql.FieldPacket[]]

        connection.release()

        if (results.length > 0) {
            return results[0]
        } else {
            return null
        }
    }

    async getAll(): Promise<Iterable<T>> {
        const connection = await this.connectionPool.getConnection()

        const [results] = await connection.query(
            'SELECT * FROM ??;',
            [this.table]
        ) as [T[], mysql.FieldPacket[]]

        connection.release()

        return results
    }

    async create(command: MySQLCRUD.CreateCommand<T, identifierColumn>): Promise<T> {
        const connection = await this.connectionPool.getConnection()

        const [{ insertId }] = await connection.query(
            'INSERT INTO people SET ?;',
            [command]
        ) as [mysql.OkPacket, mysql.FieldPacket[]]

        connection.release()

        return { [this.identifierColumn]: insertId, ...command } as unknown as T
    }

    async update(command: MySQLCRUD.UpdateCommand<T, identifierColumn>): Promise<void> {
        const connection = await this.connectionPool.getConnection()

        const identifer: number = command[this.identifierColumn]

        let properties = { ...command }
        delete properties[this.identifierColumn]

        await connection.query(
            'UPDATE ?? SET ? WHERE ?? = ?',
            [this.table, properties, this.identifierColumn, identifer]
        )

        console.log(identifer)

        connection.release()
    }

    async delete(command: MySQLCRUD.DeleteCommand<T, identifierColumn>): Promise<void> {
        const connection = await this.connectionPool.getConnection()

        const identifer: number = command[this.identifierColumn]

        await connection.query(
            'DELETE FROM ?? WHERE ?? = ? LIMIT 1;',
            [this.table, this.identifierColumn, identifer]
        )

        connection.release()
    }

    async query(command: MySQLCRUD.QueryCommand<T>): Promise<T[]> {
        let sql = 'SELECT * FROM ?? WHERE'
        let parameters: any[] = [this.table]
        let first = true

        for(const key in command) {
            const comparison = <MySQLCRUD.Comparator<T[keyof T]>>command[key]

            if(!first) sql += ' AND'
            
            first = false

            switch(comparison.comparator) {
                case MySQLCRUD.ComparatorKind.Equal:
                sql += ' ?? = ?'
                break
                case MySQLCRUD.ComparatorKind.NotEqual:
                sql += ' ?? != ?'
                break
                case MySQLCRUD.ComparatorKind.GreaterThan:
                sql += ' ?? > ?'
                break
                case MySQLCRUD.ComparatorKind.GreaterThanOrEqualTo:
                sql += ' ?? >= ?'
                break
                case MySQLCRUD.ComparatorKind.LesserThan:
                sql += ' ?? < ?'
                break
                case MySQLCRUD.ComparatorKind.LesserThanOrEqualTo:
                sql += ' ?? <= ?'
                break
                case MySQLCRUD.ComparatorKind.In:
                sql += ' ?? IN (?)'
                break
                case MySQLCRUD.ComparatorKind.NotIn:
                sql += ' ?? NOT IN (?)'
                break
                case MySQLCRUD.ComparatorKind.Like:
                sql += ' ?? LIKE ?'
                break
                case MySQLCRUD.ComparatorKind.NotLike:
                sql += ' ?? NOT LIKE ?'
                break
            }

            parameters = [...parameters, key, comparison.value]
        }

        sql += ';'

        const connection = await this.connectionPool.getConnection()
        
        const [results] = await connection.query(
            sql,
            parameters
        ) as [mysql.RowDataPacket[], mysql.FieldPacket[]]

        connection.release()

        return results as T[]
    }
}

namespace MySQLCRUD {
    export type CreateCommand<T, identifierColumn extends keyof T> = Pick<T, Exclude<keyof T, identifierColumn>>
    export type UpdateCommand<T, identifierColumn extends keyof T> = Pick<T, Extract<keyof T, identifierColumn>> & Partial<Pick<T, Exclude<keyof T, identifierColumn>>>
    export type DeleteCommand<T, identifierColumn extends keyof T> = Pick<T, Extract<keyof T, identifierColumn>>

    export enum ComparatorKind {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqualTo,
        LesserThan,
        LesserThanOrEqualTo,
        In,
        NotIn,
        Like,
        NotLike
    }
    
    export interface Equal<T> {
        comparator: ComparatorKind.Equal
        value: T
    }

    export interface NotEqual<T> {
        comparator: ComparatorKind.NotEqual
        value: T
    }

    export interface GreaterThan<T> {
        comparator: ComparatorKind.GreaterThan,
        value: T
    }

    export interface GreaterThanOrEqualTo<T> {
        comparator: ComparatorKind.GreaterThanOrEqualTo,
        value: T
    }

    export interface LesserThan<T> {
        comparator: ComparatorKind.LesserThan,
        value: T
    }
    
    export interface LesserThanOrEqualTo<T> {
        comparator: ComparatorKind.LesserThanOrEqualTo,
        value: T
    }

    export interface In<T> {
        comparator: ComparatorKind.In,
        value: T[]
    }

    export interface NotIn<T> {
        comparator: ComparatorKind.NotIn,
        value: T[]
    }

    export interface Like {
        comparator: ComparatorKind.Like,
        value: String
    }

    export interface NotLike {
        comparator: ComparatorKind.NotLike,
        value: String
    }

    export type Comparator<T> = Equal<T> | NotEqual<T> | GreaterThan<T> | GreaterThanOrEqualTo<T> | LesserThan<T> | LesserThanOrEqualTo<T> | In<T> | NotIn<T> | Like | NotLike

    export type QueryCommand<T> = AtLeastOne<{[K in keyof T]: Comparator<T[K]>}>
}

export default MySQLCRUD
