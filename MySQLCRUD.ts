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
                case MySQLCRUD.Comparator.Kind.Equal:
                sql += ' ?? = ?'
                break
                case MySQLCRUD.Comparator.Kind.NotEqual:
                sql += ' ?? != ?'
                break
                case MySQLCRUD.Comparator.Kind.GreaterThan:
                sql += ' ?? > ?'
                break
                case MySQLCRUD.Comparator.Kind.GreaterThanOrEqualTo:
                sql += ' ?? >= ?'
                break
                case MySQLCRUD.Comparator.Kind.LesserThan:
                sql += ' ?? < ?'
                break
                case MySQLCRUD.Comparator.Kind.LesserThanOrEqualTo:
                sql += ' ?? <= ?'
                break
                case MySQLCRUD.Comparator.Kind.In:
                sql += ' ?? IN (?)'
                break
                case MySQLCRUD.Comparator.Kind.NotIn:
                sql += ' ?? NOT IN (?)'
                break
                case MySQLCRUD.Comparator.Kind.Like:
                sql += ' ?? LIKE ?'
                break
                case MySQLCRUD.Comparator.Kind.NotLike:
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

    export namespace Comparator {
        export enum Kind {
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
            comparator: Kind.Equal
            value: T
        }
    
        export interface NotEqual<T> {
            comparator: Kind.NotEqual
            value: T
        }
    
        export interface GreaterThan<T> {
            comparator: Kind.GreaterThan,
            value: T
        }
    
        export interface GreaterThanOrEqualTo<T> {
            comparator: Kind.GreaterThanOrEqualTo,
            value: T
        }
    
        export interface LesserThan<T> {
            comparator: Kind.LesserThan,
            value: T
        }
        
        export interface LesserThanOrEqualTo<T> {
            comparator: Kind.LesserThanOrEqualTo,
            value: T
        }
    
        export interface In<T> {
            comparator: Kind.In,
            value: T[]
        }
    
        export interface NotIn<T> {
            comparator: Kind.NotIn,
            value: T[]
        }
    
        export interface Like {
            comparator: Kind.Like,
            value: String
        }
    
        export interface NotLike {
            comparator: Kind.NotLike,
            value: String
        }
    }

    export type Comparator<T> = Comparator.Equal<T> | Comparator.NotEqual<T> | Comparator.GreaterThan<T> | Comparator.GreaterThanOrEqualTo<T> | Comparator.LesserThan<T> | Comparator.LesserThanOrEqualTo<T> | Comparator.In<T> | Comparator.NotIn<T> | Comparator.Like | Comparator.NotLike

    export type QueryCommand<T> = AtLeastOne<{[K in keyof T]: Comparator<T[K]>}>
}

export default MySQLCRUD
