from src.postgres_data_capture import postgres_data_capture
import mock


@mock.patch('psycopg2.connect')
def test_my_function(mocked_db):

    mocked_cursor = mocked_db.return_value.cursor.return_value

    description_mock = mock.PropertyMock()
    type(mocked_cursor).description = description_mock

    fetchall_return_one = [('shanky', '347539593')]

    fetchall_return_two = [('nirma', 12313)]

    descriptions = [
        [['name'],['phone']],
        [['name'],['id']]
    ]

    mocked_cursor.fetchall.side_effect = [fetchall_return_one, fetchall_return_two]

    description_mock.side_effect = descriptions

    ret = postgres_data_capture()

    # assert whether called with mocked side effect objects
    mocked_db.assert_has_calls(mocked_cursor.fetchall.side_effect)

    # assert db query count is 2
    assert mocked_db.return_value.cursor.return_value.execute.call_count == 2

    # first query
    query1 = """
            SELECT name,phone FROM customer WHERE name='shanky'
            """
    assert mocked_db.return_value.cursor.return_value.execute.call_args_list[0][0][0] == query1

    # second query
    query2 = """
            SELECT name,id FROM product WHERE name='soap'
            """
    assert mocked_db.return_value.cursor.return_value.execute.call_args_list[1][0][0] == query2

    # assert the data of response
    assert ret == {'name' : 'nirma', 'id' : 12313}