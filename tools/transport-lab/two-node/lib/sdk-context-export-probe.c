/* Compile/link only. Never executed and never initializes a device/provider. */
#include <urma_api.h>
int main(void)
{
    urma_status_t (*volatile get_jetty)(urma_jetty_t *, urma_rjetty_t **, uint32_t *) = urma_get_rjetty;
    void (*volatile put_jetty)(urma_rjetty_t *) = urma_put_rjetty;
    urma_status_t (*volatile get_seg)(urma_target_seg_t *, urma_seg_t **, uint32_t *) = urma_get_seg_ctx;
    void (*volatile put_seg)(urma_seg_t *) = urma_put_seg_ctx;
    return get_jetty && put_jetty && get_seg && put_seg ? 0 : 1;
}
