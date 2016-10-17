package com.kazi.job.impl;

import com.kazi.job.IJob;

/**
 * Adapts a IJob interface to the Runnable.
 */
public class IJobRunnableAdapter implements Runnable {

    private final IJob job;

    public IJobRunnableAdapter(IJob job) {
        this.job = job;
    }

    @Override
    public void run() {
        job.execute();
    }
}
