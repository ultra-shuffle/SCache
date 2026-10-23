/* Compile/link only, using the selected SDK's enums, never guessed opcodes. */
#include <urma_api.h>
#include <urma_ubagg.h>
int main(void)
{
    bondp_set_bonding_mode_in_t cfg = {
        .bonding_mode = BONDP_BONDING_MODE_STANDALONE,
        .bonding_level = BONDP_BONDING_LEVEL_IODIE,
    };
    urma_status_t (*volatile control)(urma_context_t *, urma_user_ctl_in_t *, urma_user_ctl_out_t *) = urma_user_ctl;
    return control ? (int)cfg.bonding_mode + (int)cfg.bonding_level +
        (int)BONDP_USER_CTL_SET_BONDING_MODE : 1;
}
