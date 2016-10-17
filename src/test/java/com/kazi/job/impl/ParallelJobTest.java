package com.kazi.job.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.kazi.job.IJob;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Base JUnit tests for {@link ParallelJob}
 */
@RunWith(MockitoJUnitRunner.class)
public class ParallelJobTest {

    @Mock
    private IJob mockJob;

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenListIsNull() throws Exception {
        // given
        List<IJob> jobs = null;
        // when
        new ParallelJob(jobs);
        // then
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenEmptyList() throws Exception {
        // given
        List<IJob> jobs = Collections.emptyList();
        // when
        new ParallelJob(jobs);
        // then
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenPoolSizeIsNegative() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(mockJob);
        // when
        new ParallelJob(jobs, -1);
        // then
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenPoolSizeIsZero() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(mockJob);
        // when
        new ParallelJob(jobs, 0);
        // then
    }

    @Test
    public void shouldExecuteSingleJob() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(mockJob);
        ParallelJob job = new ParallelJob(jobs);
        // when
        job.execute();
        // then
        verify(mockJob, times(1)).execute();
    }

    @Test
    public void shouldAllowMaxIntAsPoolSize() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(mockJob);
        ParallelJob job = new ParallelJob(jobs, Integer.MAX_VALUE);
        // when
        job.execute();
        // then
        verify(mockJob, times(1)).execute();
    }

    @Test
    public void allJobsShouldBeExecuted() throws Exception {
        // given
        int jobsNumber = 100;
        List<StateJob> jobs = new ArrayList<>(jobsNumber);
        for (int i = 0; i < jobsNumber; i++) {
            jobs.add(new StateJob());
        }
        ParallelJob job = new ParallelJob(jobs, 1);
        // when
        job.execute();
        // then
        for (StateJob sj : jobs) {
            Assert.assertTrue(sj.isExecuted());
        }
    }

    @Test
    public void shouldExecuteManyTimeTheSameJob() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(mockJob, mockJob, mockJob, mockJob);
        ParallelJob job = new ParallelJob(jobs);
        // when
        job.execute();
        // then
        verify(mockJob, times(4)).execute();
    }

    @Test(expected = IllegalStateException.class)
    public void failingJobShouldStopProcessing() throws Exception {
        // given
        List<IJob> jobs = Arrays.asList(new FailingJob());
        ParallelJob job = new ParallelJob(jobs);
        // when
        job.execute();
        // then
    }


    private class FailingJob implements IJob {

        @Override
        public void execute() {
            throw new IllegalStateException("Failing Job exception");
        }
    }

    private class StateJob implements IJob {
        private boolean executed;

        @Override
        public void execute() {
            executed = true;
        }

        public boolean isExecuted() {
            return executed;
        }
    }

}
