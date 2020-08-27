import struct
import logging


class Message:
    """
    Base class for a message in the postgres protocol.
    Can be used to either construct a message from
    arguments or by unpacking one from the received data.
    """

    def __init__(self, struct_type, indicator, message=None):
        self.struct_type = struct_type
        self.indicator = indicator
        self.message = message

        logging.debug(f"Creating {type(self).__name__}")
        print(self.struct_type, self.indicator, self.message)

    def to_struct(self):
        """
        Return the message as a packed struct in binary format.
        The postgres protocol (see https://www.postgresql.org/docs/9.3/protocol-message-formats.html)
        typically foresees an indicator byte,
        the message length (without that byte) and the message itself.
        Exception: if there is no message, only the indicator will be sent.
        """
        if self.message:
            # Reduce by one, as indicator is not counted
            return struct.pack(
                self.struct_type,
                self.indicator,
                struct.calcsize(self.struct_type) - 1,
                *self.message,
            )
        else:
            return struct.pack(self.struct_type, self.indicator)


class ReadyForQueryMessage(Message):
    """
    ReadyForQuery

    Byte1('Z')
        Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.

    Int32(5)
        Length of message contents in bytes, including self.

    Byte1
        Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block); 'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is ended).
    """

    def __init__(self):
        super().__init__("!cic", b"Z", [b"I"])


class AuthenticationOkMessage(Message):
    """
    AuthenticationOk

    Byte1('R')
        Identifies the message as an authentication request.

    Int32(8)
        Length of message contents in bytes, including self.

    Int32(0)
        Specifies that the authentication was successful.
    """

    def __init__(self):
        super().__init__("!cii", b"R", [0])


class AuthenticationPassworkOkMessage(Message):
    """
    AuthenticationPasswordOk

    Byte1('R')
        Identifies the message as an authentication request.

    Int32(8)
        Length of message contents in bytes, including self.

    Int32(3)
        Specifies that a clear-text password is required.
    """

    def __init__(self):
        super().__init__("!cii", b"R", [3])


class SSLRequest(Message):
    """
    SSLRequest (F)
        Int32(8)
        Length of message contents in bytes, including self.

    Int32(80877103)
        The SSL request code. The value is chosen to contain 1234 in the most significant 16 bits, and 5679 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol version number.)
    """

    @staticmethod
    def from_data(data):
        msglen, sslcode = struct.unpack("!ii", data)
        assert msglen == 8
        assert sslcode == 80877103
        return True


class StartupMessage(Message):
    """
    Int32
        Length of message contents in bytes, including self.

    Int32(196608)
        The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described here).

        The protocol version number is followed by one or more pairs of parameter name and value strings. A zero byte is required as a terminator after the last name/value pair. Parameters can appear in any order. user is required, others are optional. Each parameter is specified as:

    String
        The parameter name. Currently recognized names are:

        user
        The database user name to connect as. Required; there is no default.

        database
        The database to connect to. Defaults to the user name.

        options
        Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.)

        In addition to the above, other parameters may be listed. Parameter names beginning with _pq_. are reserved for use as protocol extensions, while others are treated as run-time parameters to be set at backend start time. Such settings will be applied during backend start (after parsing the command-line arguments if any) and will act as session defaults.

    String
        The parameter value.
    """

    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def from_data(data):
        assert len(data) >= 8
        message_len, protoversion = struct.unpack("!ii", data[0:8])

        assert message_len == len(data)
        parameters_string = data[8:]
        parameters_string = [p.decode() for p in parameters_string.split(b"\x00")]

        keys = filter(None, parameters_string[::2])
        values = filter(None, parameters_string[1::2])
        return protoversion, dict(zip(keys, values))


class EmptyNoticeMessage(Message):
    """
    NoticeResponse

    Byte1('N')
        Identifies the message as a notice.

    Int32
        Length of message contents in bytes, including self.

        The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:

    Byte1
        A code identifying the field type; if zero, this is the message terminator and no string follows. The presently defined field types are listed in Section 48.6. Since more field types might be added in future, frontends should silently ignore fields of unrecognized type.

    String
        The field value.
    """

    def __init__(self):
        super().__init__("!c", b"N")


class QueryMessage(Message):
    """
    Byte1('Q')
        Identifies the message as a simple query.

    Int32
        Length of message contents in bytes, including self.

    String
        The query string itself.

    or

    Byte1('X')
        Identifies the message as a termination.

    Int32(4)
        Length of message contents in bytes, including self.
    """

    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def from_data(data):
        msgident, msglen = struct.unpack("!ci", data[0:5])

        assert msgident == b"Q"
        assert msglen == len(data) - 1
        return [query.decode() for query in data[5:].split(b"\x00") if query]


class AbortMessage(Message):
    """
    Byte1('X')
        Identifies the message as a termination.

    Int32(4)
        Length of message contents in bytes, including self.
    """

    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def from_data(data):
        msgident, msglen = struct.unpack("!ci", data[0:5])

        assert msgident == b"X"
        assert msglen == len(data)
        return False


class RowDescriptionMessage(Message):
    """
    Byte1('T')
        Identifies the message as a row description.

    Int32
        Length of message contents in bytes, including self.

    Int16
        Specifies the number of fields in a row (can be zero).

        Then, for each field, there is the following:

        String
            The field name.

        Int32
            If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

        Int16
            If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

        Int32
            The object ID of the field's data type.

        Int16
            The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

        Int32
            The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

        Int16
            The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
    """

    def __init__(self, df):
        fieldnames = [str(col).encode() for col in df.columns]

        struct_type = "!cih"
        fields = [len(fieldnames)]
        for col in fieldnames:
            struct_type += f"{len(col) + 1}sihihih"
            fields += self._column_description(col)

        super().__init__(struct_type, b"T", fields)

    def _column_description(self, column_name):
        tableid = 0
        columnid = 0
        datatypeid = 23  # TODO (23 = int4)
        datatypesize = 4  # TODO (4 bytes)
        typemodifier = -1  # TODO
        format_code = 0  # 0=text 1=binary
        return (
            column_name + b"\x00",
            tableid,
            columnid,
            datatypeid,
            datatypesize,
            typemodifier,
            format_code,
        )


class RowMessage(Message):
    """
    Byte1('D')
        Identifies the message as a data row.

    Int32
        Length of message contents in bytes, including self.

    Int16
        The number of column values that follow (possibly zero).

        Next, the following pair of fields appear for each column:

    Int32
        The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

    Byten
        The value of the column, in the format indicated by the associated format code. n is the above length.
    """

    def __init__(self, row):
        struct_type = "!cih"
        fields = [len(row)]

        for field in row:
            # TODO: we just convert to string. Not sure if this is the best thing to do
            field_content = str(field).encode()
            field_len = len(field_content)
            struct_type += f"i{field_len}s"
            fields += [field_len, field_content]

        super().__init__(struct_type, b"D", fields)


class CommandCompleteMessage(Message):
    """
    Byte1('C')
        Identifies the message as a command-completed response.

    Int32
        Length of message contents in bytes, including self.

    String
        The command tag. This is usually a single word that identifies which SQL command was completed.

        For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid is the object ID of the inserted row if rows is 1 and the target table has OIDs; otherwise oid is 0.

        For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.

        For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.

        For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.

        For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.

        For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.

        For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only in PostgreSQL 8.2 and later.)
    """

    def __init__(self, df):
        msg = f"SELECT {len(df)}\x00".encode()
        struct_type = f"!ci{len(msg)}s"

        super().__init__(struct_type, b"C", [msg])
