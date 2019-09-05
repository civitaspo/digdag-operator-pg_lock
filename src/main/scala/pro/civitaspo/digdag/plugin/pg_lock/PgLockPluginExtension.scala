package pro.civitaspo.digdag.plugin.pg_lock


import java.util.{List => JList}

import com.google.common.collect.ImmutableList
import com.google.inject.Module
import io.digdag.spi.Extension


class PgLockPluginExtension
    extends Extension
{
    override def getModules: JList[Module] =
    {
        ImmutableList.of(new PgLockPluginModule)
    }
}
