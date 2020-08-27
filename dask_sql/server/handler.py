import socketserver
import logging

from dask_sql.context import Context
from dask_sql.server import messages

logging.basicConfig(level=logging.DEBUG)


class ObservableHandler(socketserver.BaseRequestHandler):
    def read_socket(self):
        logging.debug("Start listening for new data...")
        data = self.request.recv(1024)
        logging.debug("... received {} bytes: {}".format(len(data), repr(data)))
        return data

    def read_message(self, *classes):
        data = self.read_socket()
        for cls in classes:
            try:
                logging.debug(f"Trying to cast as {cls.__name__}")
                return cls.from_data(data)
            except Exception as e:
                logging.debug(f"Failed!")
                pass

    def send_to_socket(self, data):
        logging.debug("Sending {} bytes: {}".format(len(data), repr(data)))
        return self.request.sendall(data)

    def send_message(self, message):
        return self.send_to_socket(message.to_struct())


class Handler(ObservableHandler):
    """
    Answer the postgres wire protocol by giving the SQL query
    to the dask_sql context and sending back the result.

    Idea taken from https://stackoverflow.com/questions/335008/creating-a-custom-odbc-driver

    The protocol is described here: https://www.postgresql.org/docs/9.3/protocol-flow.html
    """

    def handle(self):
        logging.debug("Handling new connection")

        self.read_message(messages.SSLRequest)
        self.send_message(messages.EmptyNoticeMessage())

        protocol_version, parameters = self.read_message(messages.StartupMessage)
        logging.debug(f"Protocol: {protocol_version}, parameters: {parameters}")

        # TODO: one could implement password authentication here
        # or at least have different contexts for different users.
        # So far we just send out "OK" without authentication
        self.send_message(messages.AuthenticationOkMessage())

        c = Context()

        while True:
            self.send_message(messages.ReadyForQueryMessage())
            query = self.read_message(messages.QueryMessage, messages.AbortMessage)
            if not query:
                # The user has aborted
                break

            try:
                # Execute the query agains the context
                result = self._execute_query(query, c)

                # Send out the result
                self._send_out_result(result)
            except:
                # TODO
                pass

    def _execute_query(self, query, c):
        assert len(query) == 1
        query = query[0]
        query = query.rstrip("; ")

        df = c.sql(query)
        result = df.compute()
        return result

    def _send_out_result(self, result):
        self.send_message(messages.RowDescriptionMessage(result))
        for index, row in result.iterrows():
            self.send_message(messages.RowMessage(row))

        self.send_message(messages.CommandCompleteMessage(result))


if __name__ == "__main__":
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("localhost", 9876), Handler) as server:
        server.serve_forever()
