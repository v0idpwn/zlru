const std = @import("std");
const testing = std.testing;

const Allocator = std.mem.Allocator;
const TQ = std.TailQueue;

pub fn ZLRU(comptime KT: type, comptime VT: type) type {
    return struct {
        const Self = @This();
        pub const PutResult = struct { key: KT, value: VT };
        const HashMapValue = struct { value: VT, node: *TQ(KT).Node };

        key_list: TQ(KT),
        hashmap: std.AutoHashMap(KT, HashMapValue),
        mutex: std.Thread.Mutex,
        allocator: Allocator,
        version: i32,
        limit: u16,
        len: u16,

        pub fn init(allocator: Allocator, limit: u16) !Self {
            const hashmap = std.AutoHashMap(KT, HashMapValue).init(allocator);
            const list = TQ(KT){};

            return Self{
                .key_list = list,
                .hashmap = hashmap,
                .mutex = std.Thread.Mutex{},
                .allocator = allocator,
                .version = 1,
                .limit = limit,
                .len = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.hashmap.deinit();
            while (self.key_list.pop()) |node| {
                self.allocator.destroy(node);
            }
            self.* = undefined;
        }

        // Since both hashmap and the linked list usage aren't thread-safe, we need
        // to acquire a lock before using them. FIXME.
        pub fn put(self: *Self, key: KT, value: VT) Allocator.Error!?PutResult {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.hashmap.get(key)) |old_hash| {
                self.key_list.remove(old_hash.node);
                self.key_list.prepend(old_hash.node);
                try self.hashmap.put(key, hashmap_value(value, old_hash.node));

                return PutResult{ .value = old_hash.value, .key = key };
            } else {
                var node = try self.alloc_as_node(key);
                self.key_list.prepend(node);
                try self.hashmap.put(key, hashmap_value(value, node));

                if (self.len >= self.limit) {
                    var result = self.key_list.pop().?;
                    defer self.allocator.destroy(result);
                    const removed_key = result.*.data;

                    if (self.hashmap.fetchRemove(removed_key)) |rmkv| {
                        return PutResult{
                            .value = rmkv.value.value,
                            .key = rmkv.key,
                        };
                    }
                } else {
                    self.len = self.len + 1;
                }
            }
            return null;
        }

        // Since both hashmap and the linked list usage aren't thread-safe, we need
        // to acquire a lock before using them. FIXME.
        pub fn get(self: *Self, key: KT) ?VT {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.hashmap.get(key)) |hash_value| {
                self.key_list.remove(hash_value.node);
                self.key_list.prepend(hash_value.node);
                return hash_value.value;
            } else {
                return null;
            }
        }

        // Since both hashmap and the linked list usage aren't thread-safe, we need
        // to acquire a lock before using them. FIXME.
        pub fn getLen(self: *Self) u16 {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.len;
        }

        // Conditionally compile?
        pub fn getKeyList(self: *Self) TQ(KT) {
            return self.key_list;
        }

        fn hashmap_value(value: VT, node: *TQ(KT).Node) HashMapValue {
            return HashMapValue{ .value = value, .node = node };
        }

        fn alloc_as_node(self: *Self, key: KT) !*TQ(KT).Node {
            var node = try self.allocator.create(TQ(KT).Node);
            node.data = key;

            return node;
        }
    };
}

test "initializing and deinitializing doesn't leak" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 256);
    defer zlru.deinit();
}

test "put/get behaves as hashmap" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 256);
    defer zlru.deinit();

    _ = try zlru.put(10, 20);
    try testing.expect(zlru.get(10).? == 20);

    _ = try zlru.put(20, 30);
    try testing.expect(zlru.get(20).? == 30);

    _ = try zlru.put(20, 40);
    try testing.expect(zlru.get(20).? == 40);

    try testing.expect(zlru.get(30) == null);
}

test "put increases len for inserts but not for updates" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 256);
    defer zlru.deinit();

    try testing.expect(zlru.getLen() == 0);

    _ = try zlru.put(1, 10);
    try testing.expect(zlru.getLen() == 1);

    _ = try zlru.put(2, 20);
    try testing.expect(zlru.getLen() == 2);

    _ = try zlru.put(2, 30);
    try testing.expect(zlru.getLen() == 2);
}

test "eviction" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 2);
    defer zlru.deinit();

    // Filling data
    var put_result = try zlru.put(1, 10);
    try testing.expect(put_result == null);

    put_result = try zlru.put(2, 20);
    try testing.expect(put_result == null);

    try testing.expect(zlru.getLen() == 2);

    // Adding one over limit will remove first added
    put_result = try zlru.put(3, 30);
    try testing.expect(put_result.?.key == 1);
    try testing.expect(put_result.?.value == 10);

    try testing.expect(zlru.getLen() == 2);
    try testing.expect(zlru.getKeyList().len == 2);

    try testing.expect(zlru.get(1) == null);
    try testing.expect(zlru.get(2).? == 20);
    try testing.expect(zlru.get(3).? == 30);

    _ = try zlru.put(4, 40);
    try testing.expect(zlru.get(2) == null);

    // Updating doesn't remove, but returns old value
    put_result = try zlru.put(4, 50);
    try testing.expect(put_result.?.key == 4);
    try testing.expect(put_result.?.value == 40);
}

test "reading affects eviction" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 5);
    defer zlru.deinit();

    _ = try zlru.put(1, 0);
    _ = try zlru.put(2, 0);
    _ = try zlru.put(3, 0);
    _ = try zlru.put(4, 0);
    _ = try zlru.put(5, 0);

    try testing.expect(zlru.get(1).? == 0);
    try testing.expect(zlru.get(2).? == 0);

    var put_result = try zlru.put(6, 0);
    try testing.expect(put_result.?.key == 3);
    try testing.expect(put_result.?.value == 0);
}

// Though we can't (easily) assert that it is working properly, we can
// assert that no memory leaks or sefaults happen on multithreaded stress
test "multithreaded usage proptest" {
    var zlru = try ZLRU(i32, i32).init(testing.allocator, 256);
    defer zlru.deinit();

    const thread0 = try std.Thread.spawn(.{}, propTest, .{ &zlru, 0 });
    const thread1 = try std.Thread.spawn(.{}, propTest, .{ &zlru, 1 });
    const thread2 = try std.Thread.spawn(.{}, propTest, .{ &zlru, 2 });
    const thread3 = try std.Thread.spawn(.{}, propTest, .{ &zlru, 3 });

    thread3.join();
    thread2.join();
    thread1.join();
    thread0.join();
}

fn propTest(zlru: *ZLRU(i32, i32), n: i32) !void {
    var i: i32 = 0;
    var x: i32 = 1000 * n;

    while (i < 1000) {
        _ = try zlru.put(i + x, n);
        _ = zlru.get(i + x);
        i += 1;
    }
}
