#include "directory_ops.h"

namespace jiffy {
namespace directory {

const perms perms::none(0);

const perms perms::owner_read(0400);
const perms perms::owner_write(0200);
const perms perms::owner_exec(0100);
const perms perms::owner_all(0700);

const perms perms::group_read(040);
const perms perms::group_write(020);
const perms perms::group_exec(010);
const perms perms::group_all(070);

const perms perms::others_read(04);
const perms perms::others_write(02);
const perms perms::others_exec(01);
const perms perms::others_all(07);

const perms perms::all(0777);

const perms perms::set_uid(04000);
const perms perms::set_gid(02000);
const perms perms::sticky_bit(01000);

const perms perms::mask(07777);

}
}

