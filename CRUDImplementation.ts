export default interface CRUDImplementation<T, I, CreateCommand, UpdateCommand, DeleteCommand> {
    get(identifier: I): Promise<T>,
    getAll(): Promise<Iterable<T>>,
    create(command: CreateCommand): Promise<T>,
    update(command: UpdateCommand): Promise<void>,
    delete(command: DeleteCommand): Promise<void>
}