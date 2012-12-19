package com.github.mistertea.zombiedb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.github.mistertea.zombiedb.DatabaseEngine;
import com.github.mistertea.zombiedb.DatabaseEngineManager;
import com.github.mistertea.zombiedb.IndexedDatabaseEngineManager;
import com.github.mistertea.zombiedb.thrift.TestThrift;

@Ignore public class DatabaseTestBase {
	DatabaseEngine db;
	DatabaseEngineManager dbm;
	IndexedDatabaseEngineManager idbm;

	@Test public void startServer() throws Exception {
	}

	@Test public void testRaw() throws Exception {
		Random r = new Random(1L);
		for(int test=0;test<100;test++) {
			byte[][] b = new byte[100][16];
			for(int i=0;i<100;i++) {
				r.nextBytes(b[i]);
			}
			
			for(int i=0;i<100;i++) {
				db.putBytes("MyClass", "key"+i, b[i]);
				byte[] ret = db.getBytes("MyClass", "key"+i);
				if(Arrays.equals(ret,b[i]) == false) {
					for(int a=0;a<16;a++) {
						System.out.println("ERROR: " + ((int)ret[a]) + " != " + ((int)b[i][a]));
					}
					Assert.fail();
				}
			}
		}
	}

	@Test public void testCreate() throws Exception {
		Random r = new Random(1L);
		for(int test=0;test<10000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			dbm.create(tt);
			
			TestThrift ttReturn = dbm.get(tt.getClass(), tt.id);
			
			Assert.assertEquals(tt, ttReturn);
			
			db.commit();
		}

		for(int test=0;test<10000;test++) {
			TestThrift tt = new TestThrift(String.valueOf(test), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			dbm.createWithId(tt);
			
			TestThrift ttReturn = dbm.get(tt.getClass(), tt.id);
			
			Assert.assertEquals(tt, ttReturn);

			db.commit();
		}
	}

	@Test public void testCreateIndexed() throws Exception {
		db.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<100;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "d", tt.d).size());
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "st", tt.st)));
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString).size());

			db.commit();
		}
		
		for(int test=0;test<100;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(100000 + r.nextInt()), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.createWithId(tt);
			
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "d", tt.d).size());
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "st", tt.st)));
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString).size());

			db.commit();
		}

		for(int test=0;test<100;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(200000 + r.nextInt()), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), null, "abc");
			idbm.createWithId(tt);
			
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "d", tt.d).size());
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "st", tt.st).size());
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString).size());

			db.commit();
		}
		
		db.wipeDatabase();
	}
	
	@Test public void testCreateIndexedSpeed() throws Exception {
		db.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			
			db.commit();
		}
		
		for(int test=0;test<1000;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(100000 + r.nextInt()), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.createWithId(tt);
			
			db.commit();
		}

		db.wipeDatabase();
	}
	
	@Test public void testCreateDeleteIndexed() throws Exception {
		db.wipeDatabase();
		
		Random r = new Random(1L);
		List<TestThrift> tts = new ArrayList<TestThrift>();
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			tts.add(tt);
			
			db.commit();
		}
		
		for(TestThrift tt : tts) {
			int ids = idbm.secondaryGet(tt.getClass(), "id", tt.id).size();
			int bs = idbm.secondaryGet(tt.getClass(), "b", tt.b).size();
			Assert.assertTrue(ids > 0);
			Assert.assertTrue(bs > 0);
			
			TestThrift ttModified = new TestThrift(tt.id, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.delete(ttModified);
			db.commit();

			int ids2 = idbm.secondaryGet(tt.getClass(), "id", tt.id).size();
			int bs2 = idbm.secondaryGet(tt.getClass(), "b", tt.b).size();
			
			Assert.assertTrue(ids-1 == ids2);
			Assert.assertTrue(bs-1 == bs2);
		}
		
		db.wipeDatabase();
	}
	
	private TestThrift getLast(ArrayList<? extends TestThrift> arrayList) {
		return arrayList.get(arrayList.size()-1);
	}

	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";
	private String generateString(Random random, int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = CHARACTERS.charAt(random.nextInt(CHARACTERS.length()));
	    }
	    return new String(text);
	}
}
