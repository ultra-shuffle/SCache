/* Read-only diagnostic probe, compiled against the installed vendor headers.
 * Does not create contexts, register memory, or submit transfers. */
#include <stdio.h>
#include <string.h>
#include "urma_api.h"

int main(int argc, char **argv) {
    if (urma_init(NULL) != URMA_SUCCESS) {
        fprintf(stderr, "CAP_INIT_FAILED\n");
        return 2;
    }
    int failures = 0;
    for (int i = 1; i < argc; ++i) {
        urma_device_t *dev = urma_get_device_by_name(argv[i]);
        urma_device_attr_t a;
        memset(&a, 0, sizeof(a));
        if (!dev || urma_query_device(dev, &a) != URMA_SUCCESS) {
            printf("FAIL\t%s\tquery_device\n", argv[i]);
            failures++;
            continue;
        }
        printf("CAP\t%s\t%u\t%u\t%u\t%u\t%u\t%u\n", argv[i],
               (unsigned)a.dev_cap.trans_mode, a.dev_cap.rc_tp_cap.bs.rtp,
               a.dev_cap.rc_tp_cap.bs.ctp, a.dev_cap.rm_tp_cap.bs.rtp,
               a.dev_cap.rm_tp_cap.bs.ctp, a.dev_cap.max_read_size);
        printf("WRITE\t%s\t%u\n", argv[i], a.dev_cap.max_write_size);
        /* The generic JNI protocol negotiates one common SEND/READ/WRITE
         * limit. Preserve all three values for the matching stage-30 gate. */
        printf("MSG\t%s\t%llu\n", argv[i],
               (unsigned long long)a.dev_cap.max_msg_size);
        for (unsigned p = 0; p < a.port_cnt && p < MAX_PORT_CNT; p++)
            printf("PORT\t%s\t%u\t%u\n", argv[i], p, (unsigned)a.port_attr[p].state);
        uint32_t count = 0;
        urma_eid_info_t *eids = urma_get_eid_list(dev, &count);
        if (!eids) {
            printf("FAIL\t%s\tget_eid_list\n", argv[i]);
            failures++;
            continue;
        }
        for (uint32_t j = 0; j < count; j++) {
            printf("EID\t%s\t%u\t", argv[i], eids[j].eid_index);
            const unsigned char *b = (const unsigned char *)&eids[j].eid;
            for (unsigned k = 0; k < sizeof(eids[j].eid); k++) printf("%02x", b[k]);
            putchar('\n');
        }
        urma_free_eid_list(eids);
    }
    /* Record actual loader mappings, not only the requested search path. */
    FILE *maps = fopen("/proc/self/maps", "r");
    if (maps) {
        char line[4096];
        while (fgets(line, sizeof(line), maps)) {
            char *path = strchr(line, '/');
            if (path && strstr(path, "liburma")) printf("MAP\t%s", path);
        }
        fclose(maps);
    }
    (void)urma_uninit();
    return failures ? 1 : 0;
}
