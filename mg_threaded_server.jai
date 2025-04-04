
// I don't trust this file yet, don't use it
// things don't work like, if all your threads are currently in use then when it next goes to claim a thread it will probably sit forever
// and in order to fix this you have to free the thread connection inside the thread
// but you're not allowed to respond to the request inside the thread, so you'll also
// have to queue the response somewhere locked with a mutex
// and I haven't done any of this yet

mg_threaded_server :: struct {
    using endpoint_server: mg_endpoint_server;
    threads: [] Thread;
    semaphores: [] Semaphore;
    thread_states: [] mg_thread_state;
    connections: [] *mg_connection_data;
    threads_alive: [] bool;
}

mg_thread_state :: enum u8 { Available :: 0; Processing; };

mg_connection_data :: struct {
    connection_state: mg_connection_state;
    thread_index: int;
    callback: mg_endpoint_callback_data;
    request: mg_request;
    response: mg_response;
    reply: mg_result;
}

mg_connection_state :: enum u8 { Opened :: 0; ShouldReply; Replying; Replied; ClosedNotFreed; ClosedAndFreed; };

mg_threaded_server_create :: (data: *void = null) -> *mg_threaded_server {
    server := New(mg_threaded_server);
    server.endpoint_server.alive = true;
    server.endpoint_server.ctx = *context;
    server.endpoint_server.data = data;

    thread_count := get_number_of_processors();
    server.threads = NewArray(thread_count, Thread);
    server.semaphores = NewArray(thread_count, Semaphore);
    server.thread_states = NewArray(thread_count, mg_thread_state);
    server.connections = NewArray(thread_count, *mg_connection_data);
    server.threads_alive = NewArray(thread_count, bool);
    server.alive = true;

    mg_set_log_level(cast(s32) MG_LL.NONE);
    // mg_log_level = cast(s32) MG_LL.ERROR;

    thread_callback :: (thread: *Thread) -> s64 {
        server := cast(*mg_threaded_server) thread.data;
        index := -1;
        for *entry: server.threads {
            if thread == entry {
                index = it_index;
                break;
            }
        }
        assert(index >= 0, "Could not find thread.\n");

        while true {
            reset_temporary_storage();
            wait_for(*server.semaphores[index]);

            if !server.alive {
                server.threads_alive[index] = false;
                return 0;
            }

            connection_data := server.connections[index];
            connection_data.reply = connection_data.callback(*connection_data.request, *connection_data.response, server.endpoint_server.data);

            if !compare_and_swap(*connection_data.connection_state, .Opened, .ShouldReply) {
                // if we couldn't set the state to should reply, that means we're closed not freed
            // TODO I removed this because it was crashing but almost certainly that's not the right thing to do TODO TODO TODO TODO TODO TODO/
            // TODO crash location was null pointer at should_close := compare_and_swap(*connection_data.connection_state, .Opened, .ClosedNotFreed);
                did_free := maybe_free_connection_data(connection_data);
                if did_free {
                    print("freeing a %\n", index);
                    free(connection_data);
                }
            }

            print("setting % to null\n", index);

            server.connections[index] = null;
            server.thread_states[index] = .Available;
        }

        return 0;
    }

    for i: 0..thread_count - 1 {
        server.threads[i].data = server;

        thread_init(*server.threads[i], thread_callback);
        init(*server.semaphores[i]);
        server.thread_states[i] = .Available;
        server.threads_alive[i] = true;
    }

    return server;
}

// this only gets called on close
// or when the thread fails to write because its marked as closed
maybe_free_connection_data :: (connection_data: *mg_connection_data) -> did_free: bool {
    start := seconds_since_init();

    while true {
        should_close := compare_and_swap(*connection_data.connection_state, .Opened, .ClosedNotFreed);

        if should_close {
            return false;
        } else {
            should_free := compare_and_swap(*connection_data.connection_state, .ShouldReply, .ClosedAndFreed);
            should_free = should_free || compare_and_swap(*connection_data.connection_state, .Replying, .ClosedAndFreed);
            should_free = should_free || compare_and_swap(*connection_data.connection_state, .Replied, .ClosedAndFreed);
            should_free = should_free || compare_and_swap(*connection_data.connection_state, .ClosedNotFreed, .ClosedAndFreed);

            if should_free {
                print("freeing c\n");
                mg_free(connection_data);

                return true;
            }
        }

        if seconds_since_init() - start > 2 {
            print("This is really bad. It's never closing.\n");
        }
    }

    return false;
}

get_connection_data :: (conn: *mg_connection) -> *mg_connection_data {
    return (cast(**mg_connection_data) conn.data.data).*;
}

set_connection_data :: (conn: *mg_connection, data: *mg_connection_data) {
    (cast(**mg_connection_data) conn.data.data).* = data;
}

mg_threaded_server_listen :: (server: *mg_threaded_server, port: u16) {
    for *thread: server.threads {
        thread_start(thread);
    }

    process_message :: (cb: mg_endpoint_callback_data, conn: *mg_connection, request: *mg_request, response: *mg_response, data: *void, listen_data: *void) {
        server := cast(*mg_threaded_server) listen_data;

        thread_index := 0;
        while true {
            valid_thread := compare_and_swap(*server.thread_states[thread_index], .Available, .Processing);
            if valid_thread {
                break;
            }

            thread_index = (thread_index + 1) % server.threads.count;
            if thread_index == 0 {
                sleep_milliseconds(1);
            }
        }

        // set_connection_state(conn, .is_hexdumping, false);

        connection_data := get_connection_data(conn);
        if !connection_data {
            connection_data = New(mg_connection_data);
            set_connection_data(conn, connection_data);
        }

        connection_data.* = .{};

        connection_data.connection_state = .Opened;
        connection_data.thread_index = thread_index;
        connection_data.callback = cb;
        connection_data.request = mg_copy_request(request.*);
        connection_data.response = mg_copy_response(response.*);

        // we're found an available thread so we can safely set it to processing
        server.connections[thread_index] = connection_data;

        signal(*server.semaphores[thread_index]);
    }

    process_poll :: (conn: *mg_connection, listen_data: *void) {
        server := cast(*mg_threaded_server) listen_data;

        // conneciton data is now always valid after the first message until the close event
        if conn {
            connection_data := get_connection_data(conn);

            if connection_data && compare_and_swap(*connection_data.connection_state, .ShouldReply, .Replying) {
                print("replying and then draining\n");

                reply := connection_data.reply;
                mg_http_reply_CFormat(conn, reply.status, reply.header, "%s", reply.body);

                set_connection_state(conn, .is_draining, true);
            }
        }
    }

    process_write :: (conn: *mg_connection, listen_data: *void) {
        connection_data := get_connection_data(conn);

        print("write\n");

        // if connection_data && compare_and_swap(*connection_data.connection_state, .Replying, .Replied) {
        //     // maybe_free_connection_data(connection_data);
        //     // TODO I need a free not closed event for this one
        //     // set_connection_state(conn, .is_closing, true);
        // }
    }

    process_close :: (conn: *mg_connection, listen_data: *void) {
        server := cast(*mg_threaded_server) listen_data;

        print("closed\n");

        connection_data := get_connection_data(conn);
        set_connection_data(conn, null);

        if connection_data {
            did_free := maybe_free_connection_data(connection_data);
            if did_free {
                print("freeing b\n");
                free(connection_data);
            }
        }
    }

    process_error :: (conn: *mg_connection, listen_data: *void) {
        print("error?\n");
        set_connection_state(conn, .is_closing, true);
    }

    mg_endpoint_server_listen(*server.endpoint_server, port, process_message, process_poll, process_write, process_close, process_error, server);
}

mg_threaded_server_close :: (server: *mg_threaded_server) {
    mg_endpoint_server_close(*server.endpoint_server);
    // definitely not right yet
}

mg_threaded_server_free :: (server: *mg_threaded_server) {
    mg_endpoint_server_free(server.endpoint_server);
    array_free(server.threads);
    array_free(server.semaphores);
    array_free(server.thread_states);
    array_free(server.connections);
    array_free(server.threads_alive);
    free(server);
}

mg_add_endpoint :: (server: *mg_threaded_server, method: mg_method, $path: string, $cb: mg_endpoint_callback_data) {
    mg_add_endpoint(*server.endpoint_server, method, path, cb);
}

mg_add_endpoint :: (server: *mg_threaded_server, method: mg_method, $path: string, $cb: mg_endpoint_callback) {
    mg_add_endpoint(*server.endpoint_server, method, path, cb);
}

mg_threaded_server_poll :: (server: *mg_threaded_server, interval: s32 = 50) {
    mg_endpoint_server_poll(*server.endpoint_server, interval);
}

mg_threaded_server_poll_async :: (server: *mg_threaded_server, $interval: s32 = 50) {
    mg_endpoint_server_poll_async(*server.endpoint_server, interval);
}

#scope_file

compare_and_swap_flags :: (dest: *$T, old: T, new: T) -> (success: bool, actual_old_value: T) {
    SIZE :: size_of(T);
    #assert SIZE == 1 || SIZE == 2 || SIZE == 4 "Invalid size.\n"; // im not sure if 8 is allowed without atomic_load

    while true {
        current := dest.*;

        if !(current & old || current == old) {
            return false, current;
        }

        actual_old := lock_cmpxchg(dest, current, new);

        if actual_old == current {
            return true, actual_old;
        }
    }

    return false, old;
}

mg_strfree :: (str: mg_str) {
    c_free(str.buf);
}

mg_copy_response :: (response: mg_response) -> mg_response {
    result: mg_response;
    result.rem = response.rem;
    result.headers = response.headers;
    return result;
}

mg_copy_request :: (request: mg_request) -> mg_request {
    result: mg_request;
    result.hm = New(mg_http_message);
    result.hm.method = mg_strdup(request.hm.method);
    result.hm.uri = mg_strdup(request.hm.uri);
    result.hm.query = mg_strdup(request.hm.query);
    result.hm.proto = mg_strdup(request.hm.proto);
    for header: request.hm.headers {
        result.hm.headers[it_index].name = mg_strdup(header.name);
        result.hm.headers[it_index].value = mg_strdup(header.value);
    }
    result.hm.body = mg_strdup(request.hm.body);
    result.hm.head = mg_strdup(request.hm.head);
    result.hm.message = mg_strdup(request.hm.message);
    return result;
    // mg_strdup
    // mg_strfree
}

mg_free :: (connection_data: mg_connection_data) {
    mg_free(connection_data.request);
    mg_free(connection_data.response);
    mg_free(connection_data.reply);
}

mg_free :: (response: mg_response) {
    // nothing to free
}

mg_free :: (request: mg_request) {
    mg_strfree(request.hm.method);
    mg_strfree(request.hm.uri);
    mg_strfree(request.hm.query);
    mg_strfree(request.hm.proto);
    for header: request.hm.headers {
        mg_strfree(header.name);
        mg_strfree(header.value);
    }
    mg_strfree(request.hm.body);
    mg_strfree(request.hm.head);
    mg_strfree(request.hm.message);
    free(request.hm);
}

c_free :: (memory: *void) #foreign crt "free";

#import "Atomics";

#if OS == .WINDOWS {
    crt :: #system_library "msvcrt";  // For malloc, free on Windows
} else #if OS == .MACOS || OS == .IOS || OS == .LINUX || OS == .ANDROID {
    crt :: #system_library "libc";
}
