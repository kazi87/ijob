package com.kazi.job.impl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.kazi.job.IJob;

import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

/**
 * An implementation of the IJob interface allows parallel execution for one or more IJobs;
 */
public class ParallelJob implements IJob {

    private final static Logger LOGGER = Logger.getLogger(ParallelJob.class);

    /**
     * List of jobs to be executed in parallel
     */
    private final Collection<Runnable> jobs;

    /**
     * Interface used for scheduling parallel jobs.
     */
    private ExecutorService executorService;

    /**
     * Constructor with default threadPoolSize set to '1'.
     * @param jobs - List of jobs to be executed
     */
    public ParallelJob(List<IJob> jobs) {
        this(jobs, 1);
    }

    /**
     * @param jobs - List of jobs to be executed
     * @param executorService - predefined implementation of the ExecutorService used for parallel execution.
     */
    public ParallelJob(List<IJob> jobs, ExecutorService executorService) {
        Validate.notEmpty(jobs, "A job list can not be null nor empty");
        Validate.notNull(executorService, "A executorService can not be null");
        this.executorService = executorService;
        // convert a list of IJob into a list of IJobCallableAdapter
        Function<IJob, Runnable> convertFunction = iJob -> new IJobRunnableAdapter(iJob);
        this.jobs = jobs.stream().map(convertFunction).collect(Collectors.toList());
        LOGGER.debug("Created a ParallelJob instance with " + jobs.size() + " jobs.");
    }

    /**
     * Parallel Job uses ThreadPoolExecutor with defined number of threads.
     * @param jobs - List of jobs to be executed
     * @param threadPoolSize - number of threads to be used in parallel execution.
     */
    public ParallelJob(List<? extends IJob> jobs, int threadPoolSize) {
        Validate.notEmpty(jobs, "A job list can not be null nor empty");
        Validate.isTrue(threadPoolSize > 0, "A threadPoolSize parameter has to be a positive integer");
        LOGGER.debug("Created an executor with pool size: " + threadPoolSize);
        executorService = Executors.newFixedThreadPool(threadPoolSize);
        // convert a list of IJob into a list of IJobCallableAdapter
        Function<IJob, Runnable> convertFunction = iJob -> new IJobRunnableAdapter(iJob);
        this.jobs = jobs.stream().map(convertFunction).collect(Collectors.toList());
        LOGGER.debug("Created a ParallelJob instance with " + jobs.size() + " jobs.");
    }


    @Override
    public void execute() {
        LOGGER.debug("Executing a ParallelJob...");

        try {
            List<Future> futures = jobs.stream().map(r -> executorService.submit(r)).collect(Collectors.toList());
            for (Future f : futures) {
                f.get();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Job Interrupted and will be cancelled " + e.getMessage());
        } catch (ExecutionException e) {
            executorService.shutdownNow();
            throw new IllegalStateException("Job execution failed: " + e.getMessage()
                + ". All unfinished jobs will be cancelled.", e);
        } finally {
            executorService.shutdown();
        }

        LOGGER.debug("ParallelJob done.");
    }

}
