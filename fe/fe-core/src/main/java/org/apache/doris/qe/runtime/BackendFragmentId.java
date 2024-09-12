package org.apache.doris.qe.runtime;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BackendFragmentId {
    public final long backendId;
    public final long fragmentId;
}
