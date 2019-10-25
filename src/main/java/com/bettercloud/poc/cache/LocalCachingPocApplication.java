package com.bettercloud.poc.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StreamUtils;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootApplication
public class LocalCachingPocApplication {

	public static void main(String[] args) {
		SpringApplication.run(LocalCachingPocApplication.class, args);
	}

	@Bean
	public Store noopStore() {
		return new Store() {
			@Override
			public String getKind() {
				return "NOOP";
			}

			@Override
			public void put(String key, String value) { }

			@Override
			public String get(String key) {
				return key;
			}

			@Override
			public List<Entry> findAll(String cursor, int pageSize) {
				return Collections.emptyList();
			}

			@Override
			public void delete(String key) { }
		};
	}

	@Bean
	public Store mapStore() {
		final Map<String, String> map = Maps.newHashMap();
		return new Store() {
			@Override
			public String getKind() {
				return "MapStore";
			}

			@Override
			public void put(String key, String value) {
				map.put(key, value);
			}

			@Override
			public String get(String key) {
				return map.get(key);
			}

			@Override
			public List<Entry> findAll(String cursor, int pageSize) {
				Stream<Map.Entry<String, String>> stream = map.entrySet().stream();
				if (cursor != null) {
					stream = stream
							.sorted(Comparator.comparing(Map.Entry::getKey))
							.filter(e -> e.getKey().compareTo(cursor) < 0);
				}
				return stream
						.limit(pageSize)
						.map(e -> Entry.of(e.getKey(), e.getValue()))
						.collect(Collectors.toList());
			}

			@Override
			public void delete(String key) {
				map.remove(key);
			}
		};
	}

	/*
	 * https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
	 */
	@Bean
	public RocksDB rocksDB() throws RocksDBException {
		RocksDB.loadLibrary();
		Options options = new Options().setCreateIfMissing(true);
		return RocksDB.open(options, "db/data.rocks");
	}

	@Bean
	public Store rocksStore(RocksDB rocksDB) {
		return new Store() {

			@Override
			public String getKind() {
				return "RocksDB";
			}

			@Override
			public void put(String key, String value) {
				try {
					rocksDB.put(to(key), to(value));
				} catch (RocksDBException e) {
					e.printStackTrace();
				}
			}

			@Override
			public String get(String key) {
				try {
					return to(rocksDB.get(to(key)));
				} catch (RocksDBException e) {
					e.printStackTrace();
				}
				return null;
			}

			@Override
			public List<Entry> findAll(String cursor, int pageSize) {
				RocksIterator it = rocksDB.newIterator();
				if (cursor != null) {
					it.seek(cursor.getBytes(Charset.defaultCharset()));
				}
				List<Entry> items = Lists.newArrayListWithCapacity(pageSize);
				while (it.isValid() && items.size() < pageSize) {
					items.add(Entry.of(to(it.key()), to(it.value())));
				}
				it.close();
				return items;
			}

			@Override
			public void delete(String key) {
				try {
					rocksDB.delete(to(key));
				} catch (RocksDBException e) {
					e.printStackTrace();
				}
			}

			private byte[] to(String val) {
				return val.getBytes(Charset.defaultCharset());
			}

			private String to(byte[] bytes) {
				return new String(bytes, Charset.defaultCharset());
			}
		};
	}

	@Bean
	public Connection connect() {
		// SQLite connection string
		String url = "jdbc:sqlite:db/data.sqlite";
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return conn;
	}

	@Bean
	public Store sqliteStore(Connection conn) throws SQLException {
		conn.createStatement().execute("DROP TABLE KV");
		conn.createStatement().execute("CREATE TABLE IF NOT EXISTS KV (k varchar NOT NULL PRIMARY KEY, v varchar)");
		return new Store() {
			@Override
			public String getKind() {
				return "Sqlite";
			}

			@Override
			public int constrainIterations(int iters) {
				return Math.min(iters, 2000);
			}

			@Override
			public void put(String key, String value) {
				try {
					PreparedStatement ps = conn.prepareStatement("INSERT INTO KV (k, v) VALUES (?, ?)");
					ps.setString(1, key);
					ps.setString(2, value);
					ps.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			@Override
			public String get(String key) {
				try {
					PreparedStatement ps = conn.prepareStatement("SELECT v FROM KV WHERE k = ?");
					ps.setString(1, key);
					ResultSet rs = ps.executeQuery();
					if (rs.next()) {
						return rs.getString(1);
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return null;
			}

			@Override
			public List<Entry> findAll(String cursor, int pageSize) {
				List<Entry> items = Lists.newArrayListWithCapacity(pageSize);
				try {
					PreparedStatement ps;
					if (cursor != null) {
						ps = conn.prepareStatement("SELECT k, v FROM KV WHERE k >= ? LIMIT ?");
						ps.setString(1, cursor);
						ps.setInt(2, pageSize);
					} else {
						ps = conn.prepareStatement("SELECT k, v FROM KV LIMIT ?");
						ps.setInt(1, pageSize);
					}
					ResultSet rs = ps.executeQuery();
					while (rs.next()) {
						items.add(Entry.of(rs.getString(1), rs.getString(2)));
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return items;
			}

			@Override
			public void delete(String key) {
				try {
					PreparedStatement ps = conn.prepareStatement("DELETE FROM KV WHERE k = ?");
					ps.setString(1, key);
					ps.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public static String getKey(int id) {
		UUID uuid = UUID.nameUUIDFromBytes((id + "").getBytes(Charset.defaultCharset()));
		return uuid.toString();
	}

	public static long now() {
		return new Date().getTime();
	}

	@Bean
	public CommandLineRunner demo(List<Store> stores) {
		return args -> {
			final int maxIterations = 100000;
			final int pageSize = 50;

			for (Store store : stores) {
				int iters = store.constrainIterations(maxIterations);
				System.out.println(String.format("%s (%,d)", store.getKind(), iters));

				Timer totalTimer = new Timer(store.getKind(), iters * 4);
				Timer writeTimer = new Timer("Writes", iters);

				for (int i = 0; i < iters; i++) {
					store.put(
							getKey(i),
							getKey(i + 1000)
					);
				}
				System.out.println(writeTimer.stop());

				Timer readTimer = new Timer("Reads", iters);
				for (int i = 0; i < iters; i++) {
					String value = store.get(getKey(i));
					assert(getKey(i + 1000).equals(value));
				}
				System.out.println(readTimer.stop());

				Timer scanTimer = new Timer("Scan", iters);
				boolean hasMore = true;
				String cursor = null;
				List<Entry> items;
				int count = 0;
				while (hasMore) {
					items = store.findAll(cursor, pageSize);
					count += items.size();
					hasMore = items.size() == pageSize;
					if (!items.isEmpty()) {
						cursor = items.get(items.size() - 1).getKey();
					}
				}
				System.out.println(scanTimer.stop());
				assert(count == iters);

				Timer deleteTimer = new Timer("Deletes", iters);
				for (int i = 0; i < iters; i++) {
					store.delete(getKey(i));
				}
				System.out.println(deleteTimer.stop());
				System.out.println(totalTimer.stop());
			}

		};
	}

	public interface Store {

		String getKind();

		void put(String key, String value);

		String get(String key);

		List<Entry> findAll(String cursor, int pageSize);

		void delete(String key);

		default int constrainIterations(int iters) {
			return iters;
		}
	}

	@Data
	public static class Entry {
		private final String key;
		private final String value;

		public static Entry of(String key, String value) {
			return new Entry(key, value);
		}
	}

	public static class Timer {
		private final String context;
		private final int count;
		private final Long startTime = now();
		private Long endTime;

		public Timer(String context, int count) {
			this.context = context;
			this.count = count;
		}

		public String stop() {
			this.endTime = now();
			return this.toString();
		}

		public long duration() {
			return Optional.ofNullable(this.endTime).orElseGet(() -> now()) - this.startTime;
		}

		public double rate() {
			return count * 1000.0 / duration();
		}

		@Override
		public String toString() {
			return String.format("\t%s: %,d duration: %,d ms rate: %,.2f ops/sec",
					context, count, duration(), rate());
		}
	}
}
