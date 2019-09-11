package pro.civitaspo.digdag.plugin.pg_lock


import scala.util.hashing.MurmurHash3


case class PgLockHasher(seed: Int)
{
    def hash(str: String): Int =
    {
        MurmurHash3.stringHash(str, seed)
    }
}
