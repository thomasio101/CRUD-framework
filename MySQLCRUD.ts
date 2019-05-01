import CRUDImplementation from './CRUDImplementation';
import * as mysql from 'mysql2/promise';

namespace MySQLCRUD {
    export type CreateCommand<T, identifierColumn extends keyof T> = Pick<T, Exclude<keyof T, identifierColumn>>
    export type UpdateCommand<T, identifierColumn extends keyof T> = Pick<T, Extract<keyof T, identifierColumn>> & Partial<Pick<T, Exclude<keyof T, identifierColumn>>>
    export type DeleteCommand<T, identifierColumn extends keyof T> = Pick<T, Extract<keyof T, identifierColumn>>
}

export default class MySQLCRUD<T, identifierColumn extends keyof T> implements CRUDImplementation<T, number, MySQLCRUD.CreateCommand<T, identifierColumn>, MySQLCRUD.UpdateCommand<T, identifierColumn>, MySQLCRUD.DeleteCommand<T, identifierColumn>> {
    constructor(private connectionPool: mysql.Pool, private table: string, private identifierColumn: string) { };

    async get(identifer: number): Promise<T> {
        const connection = await this.connectionPool.getConnection()
        
        const [results] = await connection.query(
            'SELECT * FROM ?? WHERE ?? = ? LIMIT 1;',
            [this.table, this.identifierColumn, identifer]
        ) as [T[], mysql.FieldPacket[]]

        connection.release()

        if(results.length > 0) {
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

        const [{insertId}] = await connection.query(
            'INSERT INTO people SET ?;',
            [command]
        ) as [mysql.OkPacket, mysql.FieldPacket[]]

        connection.release()

        return { [this.identifierColumn]: insertId, ...command } as unknown as T
    }

    async update(command: MySQLCRUD.UpdateCommand<T, identifierColumn>): Promise<void> {
        const connection = await this.connectionPool.getConnection()

        const identifer: number = command[this.identifierColumn]

        let properties = {...command}
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
}