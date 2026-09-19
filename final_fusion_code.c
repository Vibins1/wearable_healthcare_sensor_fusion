#include <stdio.h>

#include <stdint.h>

#include <stdlib.h>

#include <string.h>

#include <fcntl.h>

#include <unistd.h>

#include <errno.h>

#include <math.h>

#include <time.h>

#include <pthread.h>

#include <mqueue.h>

#include <signal.h>

#include <sched.h>

#include <sys/socket.h>

#include <sys/time.h>

#include <netinet/in.h>

#include <arpa/inet.h>

#include <hw/i2c.h>

#include <hw/io-spi.h>

#define I2C_DEVICE "/dev/i2c1"

#define SPI_DEVICE "/dev/io-spi/spi0/dev0"

#define MAX30102_ADDR 0x57

#define MPU6050_ADDR 0x68

#define ECG_CHANNEL 0

#define PPG_SIZE 500

#define ECG_SIZE 512

#define PPG_PERIOD_US 10000

#define ECG_PERIOD_US 4000

#define IMU_PERIOD_US 20000

#define FUSION_PERIOD_US 100000

#define CLI_PERIOD_US 1000000

#define QUEUE_NAME "/wearable_fusion_queue"

/* Windows laptop dashboard */

#define DASHBOARD_HOST "10.0.0.2"

#define DASHBOARD_PORT 8080

#define DASHBOARD_PATH "/api/ingest"

#define NETWORK_PERIOD_US 100000






typedef enum

{

    SENSOR_OK = 0,

    SENSOR_ERROR,

    SENSOR_TIMEOUT

} sensor_status_t;

typedef struct

{

    uint64_t timestamp;

    uint32_t red;

    uint32_t ir;

    float heart_rate;

    float spo2;

    sensor_status_t status;

} ppg_data_t;

typedef struct

{

    uint64_t timestamp;

    float ax;

    float ay;

    float az;

    float gx;

    float gy;

    float gz;

    float motion;

    sensor_status_t status;

} imu_data_t;

typedef struct

{

    uint64_t timestamp;

    int adc;

    float filtered;

    sensor_status_t status;

} ecg_data_t;

typedef struct

{

    uint64_t timestamp;

    float heart_rate;

    float spo2;

    float motion;

    float ecg;

    char activity[16];

    char health[32];

    uint32_t latency_us;

    sensor_status_t ppg_status;

    sensor_status_t imu_status;

    sensor_status_t ecg_status;

} fusion_result_t;

typedef struct

{

    pthread_mutex_t lock;

    ppg_data_t ppg;

    imu_data_t imu;

    ecg_data_t ecg;

    fusion_result_t fusion;

} shared_data_t;

typedef struct

{

    i2c_sendrecv_t hdr;

    uint8_t data[16];

} i2c_read_msg_t;

typedef struct

{

    i2c_send_t hdr;

    uint8_t data[16];

} i2c_write_msg_t;

typedef struct

{

    spi_xchng_t hdr;

    uint8_t data[3];

} spi_msg_t;

static shared_data_t shared;

static volatile int running = 1;

static int i2c_fd = -1;

static int spi_fd = -1;

static mqd_t queue_fd = (mqd_t)-1;

static uint32_t ppg_red[PPG_SIZE];

static uint32_t ppg_ir[PPG_SIZE];

static int ppg_head = 0;

static int ppg_count = 0;

static float ecg_buffer[ECG_SIZE];

static int ecg_raw_buffer[ECG_SIZE];

static int ecg_head = 0;

static int ecg_count = 0;

static float mpu_ax_offset = 0.0f;

static float mpu_ay_offset = 0.0f;

static float mpu_az_offset = 0.0f;

static float mpu_gx_offset = 0.0f;

static float mpu_gy_offset = 0.0f;

static float mpu_gz_offset = 0.0f;

static float ecg_baseline = 0.0f;

static float ecg_cal_noise = 1.0f;

static float ecg_quality = 0.0f;

static int ecg_signal_ok = 0;

static float ppg_ir_baseline = 0.0f;

static float ppg_red_baseline = 0.0f;

static float ppg_ir_noise = 0.0f;

static int ppg_calibrated = 0;

static uint64_t now_us(void)

{

    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    return ((uint64_t)ts.tv_sec * 1000000ULL) +

           ((uint64_t)ts.tv_nsec / 1000ULL);

}

static void add_us(struct timespec *ts, long us)

{

    ts->tv_nsec += us * 1000L;

    while (ts->tv_nsec >= 1000000000L)

    {

        ts->tv_sec++;

        ts->tv_nsec -= 1000000000L;

    }

}

static void wait_period(

    struct timespec *next,

    long period_us)

{

    clock_nanosleep(

        CLOCK_MONOTONIC,

        TIMER_ABSTIME,

        next,

        NULL

    );

    add_us(next, period_us);

}

static int i2c_read_reg(

    uint8_t address,

    uint8_t reg,

    uint8_t *buffer,

    uint32_t length)

{

    i2c_read_msg_t msg;

    if (length > sizeof(msg.data))

        return -1;

    memset(&msg, 0, sizeof(msg));

    msg.hdr.slave.addr = address;

    msg.hdr.slave.fmt = I2C_ADDRFMT_7BIT;

    msg.hdr.send_len = 1;

    msg.hdr.recv_len = length;

    msg.hdr.stop = 1;

    msg.data[0] = reg;

    if (devctl(

            i2c_fd,

            DCMD_I2C_SENDRECV,

            &msg,

            sizeof(msg.hdr) + length,

            NULL) != EOK)

    {

        return -1;

    }

    memcpy(buffer, msg.data, length);

    return 0;

}

static int i2c_write_reg(

    uint8_t address,

    uint8_t reg,

    uint8_t value)

{

    i2c_write_msg_t msg;

    memset(&msg, 0, sizeof(msg));

    msg.hdr.slave.addr = address;

    msg.hdr.slave.fmt = I2C_ADDRFMT_7BIT;

    msg.hdr.len = 2;

    msg.hdr.stop = 1;

    msg.data[0] = reg;

    msg.data[1] = value;

    if (devctl(

            i2c_fd,

            DCMD_I2C_SEND,

            &msg,

            sizeof(msg.hdr) + 2,

            NULL) != EOK)

    {

        return -1;

    }

    return 0;

}

static int configure_i2c(void)

{

    uint32_t speed = 400000;

    if (devctl(

            i2c_fd,

            DCMD_I2C_SET_BUS_SPEED,

            &speed,

            sizeof(speed),

            NULL) != EOK)

    {

        return -1;

    }

    return 0;

}

static int configure_spi(void)

{

    spi_cfg_t config;

    memset(&config, 0, sizeof(config));

    config.mode = 0;

    config.clock_rate = 1000000;

    if (devctl(

            spi_fd,

            DCMD_SPI_SET_CONFIG,

            &config,

            sizeof(config),

            NULL) != EOK)

    {

        return -1;

    }

    return 0;

}

static int max30102_init(void)

{

    uint8_t id;

    if (i2c_read_reg(

            MAX30102_ADDR,

            0xFF,

            &id,

            1) != 0)

    {

        printf("MAX30102 WHO_AM_I read failed\n");

        return -1;

    }

    printf("MAX30102 PART ID = 0x%02X\n", id);

    if (id != 0x15)

    {

        printf("Unexpected MAX30102 PART ID\n");

        return -1;

    }

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x09,

            0x40) != 0)

        return -1;

    usleep(100000);

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x04,

            0x00) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x05,

            0x00) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x06,

            0x00) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x08,

            0x1F) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x09,

            0x03) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x0A,

            0x27) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x0C,

            0x24) != 0)

        return -1;

    if (i2c_write_reg(

            MAX30102_ADDR,

            0x0D,

            0x24) != 0)

        return -1;

    printf("MAX30102 initialized successfully\n");

    return 0;

}

static int max30102_sample(

    uint32_t *red,

    uint32_t *ir)

{

    uint8_t wr_ptr;

    uint8_t rd_ptr;

    uint8_t fifo[6];

    if (i2c_read_reg(MAX30102_ADDR, 0x04, &wr_ptr, 1) != 0)

        return -1;

    if (i2c_read_reg(MAX30102_ADDR, 0x06, &rd_ptr, 1) != 0)

        return -1;

    if (wr_ptr == rd_ptr)

        return 1;

    if (i2c_read_reg(MAX30102_ADDR, 0x07, fifo, 6) != 0)

        return -1;

    *red = ((uint32_t)(fifo[0] & 0x03) << 16) |

           ((uint32_t)fifo[1] << 8) | fifo[2];

    *ir = ((uint32_t)(fifo[3] & 0x03) << 16) |

          ((uint32_t)fifo[4] << 8) | fifo[5];

    return 0;

}

static int max30102_calibrate(void)

{

    double red_sum = 0.0;

    double ir_sum = 0.0;

    double ir_var = 0.0;

    uint32_t red;

    uint32_t ir;

    uint32_t ir_samples[100];

    int collected = 0;

    int attempts = 0;

    printf("MAX30102 calibration: place finger gently and keep still...\n");

    while (collected < 100 && attempts < 1000)

    {

        int r = max30102_sample(&red, &ir);

        attempts++;

        if (r == 0)

        {

            red_sum += red;

            ir_sum += ir;

            ir_samples[collected] = ir;

            collected++;

        }

        usleep(PPG_PERIOD_US);

    }

    if (collected < 80)

    {

        printf("MAX30102 calibration failed: insufficient samples\n");

        return -1;

    }

    ppg_red_baseline = (float)(red_sum / collected);

    ppg_ir_baseline = (float)(ir_sum / collected);

    for (int i = 0; i < collected; i++)

    {

        double d = (double)ir_samples[i] - ppg_ir_baseline;

        ir_var += d * d;

    }

    ppg_ir_noise = (float)sqrt(ir_var / collected);

    ppg_calibrated = 1;

    printf("MAX30102 calibration complete: RED=%.0f IR=%.0f noise=%.0f\n",

           ppg_red_baseline, ppg_ir_baseline, ppg_ir_noise);

    return 0;

}

static void store_ppg(

    uint32_t red,

    uint32_t ir)

{

    ppg_red[ppg_head] = red;

    ppg_ir[ppg_head] = ir;

    ppg_head++;

    if (ppg_head >= PPG_SIZE)

        ppg_head = 0;

    if (ppg_count < PPG_SIZE)

        ppg_count++;

}

static int ppg_finger_contact(void)

{

    if (!ppg_calibrated || ppg_count < 20)

        return 0;

    int n = ppg_count < 50 ? ppg_count : 50;

    double ir_sum = 0.0;

    double red_sum = 0.0;

    for (int i = 0; i < n; i++)

    {

        int index = (ppg_head - n + i + PPG_SIZE) % PPG_SIZE;

        ir_sum += ppg_ir[index];

        red_sum += ppg_red[index];

    }

    ir_sum /= n;

    red_sum /= n;

    return (ir_sum > 5000.0 && red_sum > 5000.0);

}

static float ppg_quality(void)

{

    if (!ppg_finger_contact() || ppg_count < 100)

        return 0.0f;

    double ir_mean = 0.0;

    double ir_ac = 0.0;

    int n = ppg_count < 200 ? ppg_count : 200;

    for (int i = 0; i < n; i++)

    {

        int index = (ppg_head - n + i + PPG_SIZE) % PPG_SIZE;

        ir_mean += ppg_ir[index];

    }

    ir_mean /= n;

    for (int i = 0; i < n; i++)

    {

        int index = (ppg_head - n + i + PPG_SIZE) % PPG_SIZE;

        double d = ppg_ir[index] - ir_mean;

        ir_ac += d * d;

    }

    ir_ac = sqrt(ir_ac / n);

    if (ir_mean <= 0.0)

        return 0.0f;

    float ratio = (float)(ir_ac / ir_mean);

    if (ratio < 0.005f)

        return 0.2f;

    if (ratio > 0.12f)

        return 0.5f;

    return 1.0f;

}

static float calculate_hr(void)

{

    uint32_t samples[PPG_SIZE];

    float mean = 0.0f;

    float variance = 0.0f;

    float threshold;

    int peaks = 0;

    int last_peak = -1000;

    int i;

    if (ppg_count < 200)

        return 0.0f;

    for (i = 0; i < ppg_count; i++)

    {

        int index =

            (ppg_head - ppg_count + i + PPG_SIZE)

            % PPG_SIZE;

        samples[i] = ppg_ir[index];

        mean += (float)samples[i];

    }

    mean /= ppg_count;

    for (i = 0; i < ppg_count; i++)

    {

        float difference =

            (float)samples[i] - mean;

        variance +=

            difference * difference;

    }

    variance /= ppg_count;

    threshold =

        sqrtf(variance) * 0.45f;

    for (i = 2; i < ppg_count - 2; i++)

    {

        if (samples[i] > mean + threshold &&

            samples[i] > samples[i - 1] &&

            samples[i] >= samples[i + 1])

        {

            if (i - last_peak > 30)

            {

                peaks++;

                last_peak = i;

            }

        }

    }

    if (peaks < 2)

        return 0.0f;

    float duration =

        (float)(ppg_count - 1) / 100.0f;

    float bpm =

        ((float)(peaks - 1) / duration) *

        60.0f;

    if (bpm < 40.0f ||

        bpm > 200.0f)

        return 0.0f;

    return bpm;

}

static float calculate_spo2(void)

{

    double red_mean = 0.0;

    double ir_mean = 0.0;

    double red_ac = 0.0;

    double ir_ac = 0.0;

    double ratio;

    int i;

    if (ppg_count < 100)

        return 0.0f;

    for (i = 0; i < ppg_count; i++)

    {

        int index =

            (ppg_head - ppg_count + i + PPG_SIZE)

            % PPG_SIZE;

        red_mean += ppg_red[index];

        ir_mean += ppg_ir[index];

    }

    red_mean /= ppg_count;

    ir_mean /= ppg_count;

    if (red_mean < 1.0 ||

        ir_mean < 1.0)

        return 0.0f;

    for (i = 0; i < ppg_count; i++)

    {

        int index =

            (ppg_head - ppg_count + i + PPG_SIZE)

            % PPG_SIZE;

        double rd =

            ppg_red[index] - red_mean;

        double id =

            ppg_ir[index] - ir_mean;

        red_ac += rd * rd;

        ir_ac += id * id;

    }

    red_ac =

        sqrt(red_ac / ppg_count);

    ir_ac =

        sqrt(ir_ac / ppg_count);

    if (ir_ac <= 0.0)

        return 0.0f;

    ratio =

        (red_ac / red_mean) /

        (ir_ac / ir_mean);

    if (ratio < 0.30 || ratio > 2.00)

        return 0.0f;

    float spo2 =

        -45.060f * (float)(ratio * ratio) +

        30.354f * (float)ratio +

        94.845f;

    if (spo2 > 100.0f)

        spo2 = 100.0f;

    if (spo2 < 70.0f)

        spo2 = 70.0f;

    return spo2;

}

static int mpu6050_init(void)

{

    uint8_t id;

    printf(

        "Checking MPU6050 at I2C address 0x%02X...\n",

        MPU6050_ADDR

    );

    if (i2c_read_reg(

            MPU6050_ADDR,

            0x75,

            &id,

            1) != 0)

    {

        printf(

            "MPU6050 WHO_AM_I read failed\n"

        );

        return -1;

    }

    printf(

        "MPU6050 WHO_AM_I = 0x%02X\n",

        id

    );

    if (id != 0x68 &&

        id != 0x70 &&

        id != 0x71)

    {

        printf(

            "Unexpected MPU6050 ID\n"

        );

        return -1;

    }

    if (i2c_write_reg(

            MPU6050_ADDR,

            0x6B,

            0x00) != 0)

    {

        printf(

            "MPU6050 wake-up failed\n"

        );

        return -1;

    }

    if (i2c_write_reg(

            MPU6050_ADDR,

            0x1C,

            0x00) != 0)

    {

        printf(

            "MPU6050 accelerometer configuration failed\n"

        );

        return -1;

    }

    if (i2c_write_reg(

            MPU6050_ADDR,

            0x1B,

            0x00) != 0)

    {

        printf(

            "MPU6050 gyroscope configuration failed\n"

        );

        return -1;

    }

    printf(

        "MPU6050 initialized successfully\n"

    );

    return 0;

}

static int mpu6050_read_raw(

    int16_t *ax, int16_t *ay, int16_t *az,

    int16_t *gx, int16_t *gy, int16_t *gz)

{

    uint8_t buffer[14];

    if (i2c_read_reg(MPU6050_ADDR, 0x3B, buffer, 14) != 0)

        return -1;

    *ax = (int16_t)(((uint16_t)buffer[0] << 8) | buffer[1]);

    *ay = (int16_t)(((uint16_t)buffer[2] << 8) | buffer[3]);

    *az = (int16_t)(((uint16_t)buffer[4] << 8) | buffer[5]);

    *gx = (int16_t)(((uint16_t)buffer[8] << 8) | buffer[9]);

    *gy = (int16_t)(((uint16_t)buffer[10] << 8) | buffer[11]);

    *gz = (int16_t)(((uint16_t)buffer[12] << 8) | buffer[13]);

    return 0;

}

static int mpu6050_calibrate(void)

{

    double ax_sum = 0.0, ay_sum = 0.0, az_sum = 0.0;

    double gx_sum = 0.0, gy_sum = 0.0, gz_sum = 0.0;

    int collected = 0;

    printf("MPU6050 calibration: keep sensor completely still...\n");

    usleep(500000);

    while (collected < 100)

    {

        int16_t ax, ay, az, gx, gy, gz;

        if (mpu6050_read_raw(&ax, &ay, &az, &gx, &gy, &gz) == 0)

        {

            ax_sum += ax / 16384.0;

            ay_sum += ay / 16384.0;

            az_sum += az / 16384.0;

            gx_sum += gx / 131.0;

            gy_sum += gy / 131.0;

            gz_sum += gz / 131.0;

            collected++;

        }

        usleep(IMU_PERIOD_US);

    }

    mpu_ax_offset = (float)(ax_sum / collected);

    mpu_ay_offset = (float)(ay_sum / collected);

    mpu_az_offset = (float)(az_sum / collected) - 1.0f;

    mpu_gx_offset = (float)(gx_sum / collected);

    mpu_gy_offset = (float)(gy_sum / collected);

    mpu_gz_offset = (float)(gz_sum / collected);

    printf("MPU6050 offsets: A=(%.4f, %.4f, %.4f) G=(%.3f, %.3f, %.3f)\n",

           mpu_ax_offset, mpu_ay_offset, mpu_az_offset,

           mpu_gx_offset, mpu_gy_offset, mpu_gz_offset);

    return 0;

}

static int mpu6050_read(imu_data_t *imu)

{

    int16_t ax, ay, az, gx, gy, gz;

    if (mpu6050_read_raw(&ax, &ay, &az, &gx, &gy, &gz) != 0)

        return -1;

    imu->ax = ax / 16384.0f - mpu_ax_offset;

    imu->ay = ay / 16384.0f - mpu_ay_offset;

    imu->az = az / 16384.0f - mpu_az_offset;

    imu->gx = gx / 131.0f - mpu_gx_offset;

    imu->gy = gy / 131.0f - mpu_gy_offset;

    imu->gz = gz / 131.0f - mpu_gz_offset;

    return 0;

}

static int mcp3008_read(

    int channel)

{

    spi_msg_t msg;

    memset(

        &msg,

        0,

        sizeof(msg)

    );

    msg.hdr.nbytes = 3;

    msg.data[0] = 0x01;

    msg.data[1] =

        (uint8_t)(

            0x80 |

            ((channel & 7) << 4)

        );

    msg.data[2] = 0x00;

    if (devctl(

            spi_fd,

            DCMD_SPI_DATA_XCHNG,

            &msg,

            sizeof(msg),

            NULL) != EOK)

        return -1;

    return

        ((msg.data[1] & 0x03) << 8) |

        msg.data[2];

}

static int calibrate_ecg(void)

{

    double sum = 0.0;

    double sum2 = 0.0;

    int collected = 0;

    printf("ECG calibration: keep electrodes connected and body still...\n");

    while (collected < 500)

    {

        int adc = mcp3008_read(ECG_CHANNEL);

        if (adc >= 0)

        {

            sum += adc;

            sum2 += (double)adc * (double)adc;

            collected++;

        }

        usleep(ECG_PERIOD_US);

    }

    if (collected == 0)

        return -1;

    ecg_baseline = (float)(sum / collected);

    {

        double variance = (sum2 / collected) -

                          ((double)ecg_baseline * (double)ecg_baseline);

        if (variance < 0.0)

            variance = 0.0;

        ecg_cal_noise = (float)sqrt(variance);

        if (ecg_cal_noise < 1.0f)

            ecg_cal_noise = 1.0f;

    }

    printf("ECG baseline = %.2f, calibration noise = %.2f\n",

           ecg_baseline, ecg_cal_noise);

    return 0;

}

static float filter_ecg(float sample)

{

    static float baseline = -1.0f;

    if (baseline < 0.0f)

        baseline = ecg_baseline;

    baseline += 0.002f * (sample - baseline);

    return sample - baseline;

}

static int check_ecg_signal(void)

{

    int available = ecg_count;

    int i;

    int rail_count = 0;

    float min_v = 4095.0f;

    float max_v = -4095.0f;

    double raw_sum = 0.0;

    double raw_sum2 = 0.0;

    double filt_sum = 0.0;

    double filt_sum2 = 0.0;

    if (available < 125)

    {

        ecg_quality = 0.0f;

        return 0;

    }

    for (i = 0; i < available; i++)

    {

        int raw = ecg_raw_buffer[i];

        float v = ecg_buffer[i];

        if (raw < 15 || raw > 1008)

            rail_count++;

        if (v < min_v) min_v = v;

        if (v > max_v) max_v = v;

        raw_sum += raw;

        raw_sum2 += (double)raw * (double)raw;

        filt_sum += v;

        filt_sum2 += (double)v * (double)v;

    }

    {

        double raw_mean = raw_sum / available;

        double raw_variance = (raw_sum2 / available) -

                              (raw_mean * raw_mean);

        double filt_mean = filt_sum / available;

        double filt_variance = (filt_sum2 / available) -

                               (filt_mean * filt_mean);

        float raw_rms;

        float filt_rms;

        float range = max_v - min_v;

        float raw_noise_limit = ecg_cal_noise * 10.0f;

        if (raw_variance < 0.0)

            raw_variance = 0.0;

        if (filt_variance < 0.0)

            filt_variance = 0.0;

        raw_rms = (float)sqrt(raw_variance);

        filt_rms = (float)sqrt(filt_variance);

        /*

         * This is a prototype lead/signal-quality check. It is not a

         * clinical lead-off detector. It looks for rail saturation,

         * excessive noise, and a large change from the connected baseline.

         */

        if (rail_count > available / 10)

        {

            ecg_quality = 0.0f;

            return 0;

        }

        if (fabs(raw_mean - ecg_baseline) > 250.0f)

        {

            ecg_quality = 0.0f;

            return 0;

        }

        if (raw_rms > raw_noise_limit && raw_rms > 80.0f)

        {

            ecg_quality = 0.0f;

            return 0;

        }

        if (filt_rms > 20.0f)

        {

            ecg_quality = 0.0f;

            return 0;

        }

        if (range < 0.5f && filt_rms < 0.5f)

        {

            ecg_quality = 0.0f;

            return 0;

        }

        ecg_quality = 1.0f;

        if (raw_rms > raw_noise_limit || filt_rms > 10.0f)

            ecg_quality = 0.5f;

        return 1;

    }

}

static const char *ecg_status_text(sensor_status_t status)

{

    if (status == SENSOR_OK)

        return "SIGNAL OK";

    if (status == SENSOR_TIMEOUT)

        return "TIMEOUT";

    return "CHECK ELECTRODES";

}

static void classify_activity(

    float motion,

    char *result)

{

    if (motion < 0.015f)

        strcpy(result, "RESTING");

    else if (motion < 0.080f)

        strcpy(result, "WALKING");

    else

        strcpy(result, "ACTIVE");

}

static void classify_health(

    float hr,

    float spo2,

    sensor_status_t ppg_status,

    sensor_status_t imu_status,

    sensor_status_t ecg_status,

    char *result)

{

    if (ppg_status != SENSOR_OK ||

        imu_status != SENSOR_OK ||

        ecg_status != SENSOR_OK)

    {

        strcpy(result, "SENSOR CHECK");

        return;

    }

    if (hr <= 0.0f ||

        spo2 <= 0.0f)

    {

        strcpy(result, "WAITING");

        return;

    }

    if (spo2 < 92.0f)

    {

        strcpy(result, "ATTENTION");

        return;

    }

    if (hr > 120.0f)

    {

        strcpy(result, "HIGH HR");

        return;

    }

    strcpy(result, "NORMAL");

}

static void *max30102_task(

    void *arg)

{

    struct timespec next;

    clock_gettime(

        CLOCK_MONOTONIC,

        &next

    );

    while (running)

    {

        uint32_t red;

        uint32_t ir;

        int result =

            max30102_sample(

                &red,

                &ir

            );

        if (result == 0)

        {

            pthread_mutex_lock(

                &shared.lock

            );

            store_ppg(

                red,

                ir

            );

            shared.ppg.timestamp =

                now_us();

            shared.ppg.red =

                red;

            shared.ppg.ir =

                ir;

            shared.ppg.heart_rate =

                calculate_hr();

            shared.ppg.spo2 =

                calculate_spo2();

            (void)ppg_quality();

            shared.ppg.status =

                SENSOR_OK;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        else if (result < 0)

        {

            pthread_mutex_lock(

                &shared.lock

            );

            shared.ppg.status =

                SENSOR_ERROR;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        wait_period(

            &next,

            PPG_PERIOD_US

        );

    }

    return NULL;

}

static void *mpu6050_task(

    void *arg)

{

    struct timespec next;

    clock_gettime(

        CLOCK_MONOTONIC,

        &next

    );

    float previous = 1.0f;

    while (running)

    {

        imu_data_t imu;

        memset(

            &imu,

            0,

            sizeof(imu)

        );

        if (mpu6050_read(&imu) == 0)

        {

            float magnitude =

                sqrtf(

                    imu.ax * imu.ax +

                    imu.ay * imu.ay +

                    imu.az * imu.az

                );

            imu.motion =

                fabsf(

                    magnitude - previous

                );

            previous =

                magnitude;

            imu.timestamp =

                now_us();

            imu.status =

                SENSOR_OK;

            pthread_mutex_lock(

                &shared.lock

            );

            shared.imu =

                imu;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        else

        {

            pthread_mutex_lock(

                &shared.lock

            );

            shared.imu.status =

                SENSOR_ERROR;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        wait_period(

            &next,

            IMU_PERIOD_US

        );

    }

    return NULL;

}

static void *ecg_task(

    void *arg)

{

    struct timespec next;

    clock_gettime(

        CLOCK_MONOTONIC,

        &next

    );

    while (running)

    {

        int adc =

            mcp3008_read(

                ECG_CHANNEL

            );

        if (adc >= 0)

        {

            float filtered =

                filter_ecg(

                    (float)adc

                );

            ecg_raw_buffer[ecg_head] = adc;

            ecg_buffer[ecg_head] =

                filtered;

            ecg_head++;

            if (ecg_head >= ECG_SIZE)

                ecg_head = 0;

            if (ecg_count < ECG_SIZE)

                ecg_count++;

            ecg_signal_ok = check_ecg_signal();

            pthread_mutex_lock(

                &shared.lock

            );

            shared.ecg.timestamp =

                now_us();

            shared.ecg.adc =

                adc;

            shared.ecg.filtered =

                filtered;

            shared.ecg.status =

                ecg_signal_ok ? SENSOR_OK : SENSOR_ERROR;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        else

        {

            pthread_mutex_lock(

                &shared.lock

            );

            shared.ecg.status =

                SENSOR_ERROR;

            pthread_mutex_unlock(

                &shared.lock

            );

        }

        wait_period(

            &next,

            ECG_PERIOD_US

        );

    }

    return NULL;

}

static void *fusion_task(

    void *arg)

{

    struct timespec next;

    clock_gettime(

        CLOCK_MONOTONIC,

        &next

    );

    while (running)

    {

        fusion_result_t result;

        uint64_t start =

            now_us();

        pthread_mutex_lock(

            &shared.lock

        );

        float hr =

            shared.ppg.heart_rate;

        float spo2 =

            shared.ppg.spo2;

        float motion =

            shared.imu.motion;

        float ecg =

            shared.ecg.filtered;

        sensor_status_t ppg_status =

            shared.ppg.status;

        sensor_status_t imu_status =

            shared.imu.status;

        sensor_status_t ecg_status =

            shared.ecg.status;

        uint64_t ppg_age =

            start -

            shared.ppg.timestamp;

        uint64_t imu_age =

            start -

            shared.imu.timestamp;

        pthread_mutex_unlock(

            &shared.lock

        );

        if (ppg_age > 1000000ULL)

            ppg_status =

                SENSOR_TIMEOUT;

        if (imu_age > 500000ULL)

            imu_status =

                SENSOR_TIMEOUT;

        memset(

            &result,

            0,

            sizeof(result)

        );

        result.timestamp =

            start;

        result.heart_rate =

            hr;

        result.spo2 =

            spo2;

        result.motion =

            motion;

        result.ecg =

            ecg;

        result.ppg_status =

            ppg_status;

        result.imu_status =

            imu_status;

        result.ecg_status =

            ecg_status;

        classify_activity(

            motion,

            result.activity

        );

        classify_health(

            hr,

            spo2,

            ppg_status,

            imu_status,

            ecg_status,

            result.health

        );

        result.latency_us =

            (uint32_t)(

                now_us() - start

            );

        pthread_mutex_lock(

            &shared.lock

        );

        shared.fusion =

            result;

        pthread_mutex_unlock(

            &shared.lock

        );

        if (queue_fd !=

            (mqd_t)-1)

        {

            mq_send(

                queue_fd,

                (const char *)&result,

                sizeof(result),

                1

            );

        }

        wait_period(

            &next,

            FUSION_PERIOD_US

        );

    }

    return NULL;

}

static void *logger_task(

    void *arg)

{

    FILE *file =

        fopen(

            "/tmp/wearable_data.csv",

            "w"

        );

    if (file == NULL)

        return NULL;

    fprintf(

        file,

        "timestamp,heart_rate,spo2,motion,ecg,activity,health,latency_us,ppg_status,imu_status,ecg_status\n"

    );

    while (running)

    {

        fusion_result_t result;

        ssize_t received =

            mq_receive(

                queue_fd,

                (char *)&result,

                sizeof(result),

                NULL

            );

        if (received ==

            (ssize_t)sizeof(result))

        {

            fprintf(

                file,

                "%llu,%.2f,%.2f,%.5f,%.2f,%s,%s,%u,%d,%d,%d\n",

                (unsigned long long)

                    result.timestamp,

                result.heart_rate,

                result.spo2,

                result.motion,

                result.ecg,

                result.activity,

                result.health,

                result.latency_us,

                result.ppg_status,

                result.imu_status,

                result.ecg_status

            );

            fflush(file);

        }

        else

        {

            usleep(20000);

        }

    }

    fclose(file);

    return NULL;

}





/*

 * Dashboard networking

 *

 * QNX/Raspberry Pi: 10.0.0.1

 * Windows laptop:  10.0.0.2

 * Node.js server:  TCP 8080

 */

static int dashboard_send(const fusion_result_t *result,

                          const ppg_data_t *ppg,

                          const imu_data_t *imu)

{

    int sock = -1;

    struct sockaddr_in server_addr;

    struct timeval tv;

    char body[1024];

    char request[2048];

    char response[256];

    int body_len;

    int request_len;

    int sent_total = 0;

    int n;

    sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0)

        return -1;

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;

    server_addr.sin_port = htons(DASHBOARD_PORT);

    if (inet_pton(AF_INET, DASHBOARD_HOST, &server_addr.sin_addr) != 1)

    {

        close(sock);

        return -1;

    }

    /* 500 ms send/receive timeout */

    tv.tv_sec = 0;

    tv.tv_usec = 500000;

    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (connect(sock,

                (struct sockaddr *)&server_addr,

                sizeof(server_addr)) < 0)

    {

        close(sock);

        return -1;

    }

    body_len = snprintf(

        body,

        sizeof(body),

        "{"

        "\"timestamp_us\":%llu,"

        "\"red\":%u,"

        "\"ir\":%u,"

        "\"ax\":%.5f,"

        "\"ay\":%.5f,"

        "\"az\":%.5f,"

        "\"gx\":%.5f,"

        "\"gy\":%.5f,"

        "\"gz\":%.5f,"

        "\"motion\":%.5f,"

        "\"ecg\":%.5f,"

        "\"hr_bpm\":%.2f,"

        "\"spo2_pct\":%.2f,"

        "\"activity\":\"%s\","

        "\"health\":\"%s\","

        "\"latency_us\":%u,"

        "\"max30102_ok\":%s,"

        "\"mpu6050_ok\":%s,"

        "\"ecg_ok\":%s,"

        "\"ecg_quality\":%.2f"

        "}",

        (unsigned long long)result->timestamp,

        ppg->red,

        ppg->ir,

        imu->ax,

        imu->ay,

        imu->az,

        imu->gx,

        imu->gy,

        imu->gz,

        result->motion,

        result->ecg,

        result->heart_rate,

        result->spo2,

        result->activity,

        result->health,

        result->latency_us,

        result->ppg_status == SENSOR_OK ? "true" : "false",

        result->imu_status == SENSOR_OK ? "true" : "false",

        result->ecg_status == SENSOR_OK ? "true" : "false",

        ecg_quality

    );

    if (body_len < 0 || body_len >= (int)sizeof(body))

    {

        close(sock);

        return -1;

    }

    request_len = snprintf(

        request,

        sizeof(request),

        "POST " DASHBOARD_PATH " HTTP/1.1\r\n"

        "Host: " DASHBOARD_HOST ":8080\r\n"

        "Content-Type: application/json\r\n"

        "Content-Length: %d\r\n"

        "Connection: close\r\n"

        "\r\n"

        "%s",

        body_len,

        body

    );

    if (request_len < 0 || request_len >= (int)sizeof(request))

    {

        close(sock);

        return -1;

    }

    while (sent_total < request_len)

    {

        n = send(sock,

                 request + sent_total,

                 request_len - sent_total,

                 0);

        if (n <= 0)

        {

            close(sock);

            return -1;

        }

        sent_total += n;

    }

    /* The Node.js /api/ingest endpoint returns HTTP 200 on success. */

    n = recv(sock, response, sizeof(response) - 1, 0);

    if (n <= 0)

    {

        close(sock);

        return -1;

    }

    response[n] = '\0';

    if (strstr(response, " 200 ") == NULL)

    {

        close(sock);

        return -1;

    }

    close(sock);

    return 0;

}


static void *network_task(void *arg)

{
    struct timespec next;

    unsigned long sent_count = 0;

    unsigned long fail_count = 0;

    (void)arg;

    clock_gettime(CLOCK_MONOTONIC, &next);

    while (running)

    {

        fusion_result_t result;

        ppg_data_t ppg;

        imu_data_t imu;

        pthread_mutex_lock(&shared.lock);

        result = shared.fusion;

        ppg = shared.ppg;

        imu = shared.imu;

        pthread_mutex_unlock(&shared.lock);

        if (dashboard_send(&result, &ppg, &imu) == 0)

        {

            sent_count++;

            if (sent_count == 1 || (sent_count % 100) == 0)

            {

                printf(

                    "Dashboard connected: %s:%d (samples sent=%lu)\n",

                    DASHBOARD_HOST,

                    DASHBOARD_PORT,

                    sent_count

                );

            }

        }

        else

        {

            fail_count++;

            if (fail_count == 1 || (fail_count % 10) == 0)

            {

                printf(

                    "Dashboard POST failed: %s:%d (failures=%lu)\n",

                    DASHBOARD_HOST,

                    DASHBOARD_PORT,

                    fail_count

                );

            }

        }

        wait_period(&next, NETWORK_PERIOD_US);

    }

    return NULL;

}

static const char *status_text(

    sensor_status_t status)

{

    if (status == SENSOR_OK)

        return "OK";

    if (status == SENSOR_TIMEOUT)

        return "TIMEOUT";

    return "ERROR";

}

static void *cli_task(

    void *arg)

{

    struct timespec next;

    clock_gettime(

        CLOCK_MONOTONIC,

        &next

    );

    while (running)

    {

        fusion_result_t result;

        ppg_data_t ppg;

        imu_data_t imu;

        ecg_data_t ecg;

        pthread_mutex_lock(

            &shared.lock

        );

        result =

            shared.fusion;

        ppg =

            shared.ppg;

        imu =

            shared.imu;

        ecg =

            shared.ecg;

        pthread_mutex_unlock(

            &shared.lock

        );

        printf(

            "\033[2J\033[H"

        );

        printf(

            "Wearable Health Sensor Fusion\n"

        );

        printf(

            "==============================\n\n"

        );

        printf(

            "MAX30102 : %s\n",

            status_text(ppg.status)

        );

        printf(

            "MPU6050  : %s\n",

            status_text(imu.status)

        );

        printf(

            "ECG      : %s  (quality %.0f%%)\n\n",

            ecg_status_text(ecg.status),

            ecg_quality * 100.0f

        );

        printf(

            "Heart Rate : %.1f BPM\n",

            result.heart_rate

        );

        printf(

            "SpO2       : %.1f %%\n",

            result.spo2

        );

        printf(

            "Motion     : %.4f\n",

            result.motion

        );

        printf(

            "ECG        : %.2f\n\n",

            result.ecg

        );

        printf(

            "Activity   : %s\n",

            result.activity

        );

        printf(

            "Health     : %s\n",

            result.health

        );

        printf(

            "\nFusion latency : %u us\n",

            result.latency_us

        );

        printf(

            "Data file      : /tmp/wearable_data.csv\n"

        );

        wait_period(

            &next,

            CLI_PERIOD_US

        );

    }

    return NULL;

}

static void stop_handler(

    int signal_number)

{

    (void)signal_number;

    running = 0;

}

static int set_priority(

    pthread_t thread,

    int priority)

{

    struct sched_param param;

    memset(

        &param,

        0,

        sizeof(param)

    );

    param.sched_priority =

        priority;

    return pthread_setschedparam(

        thread,

        SCHED_FIFO,

        &param

    );

}

int main(void)

{

    pthread_t ppg_thread;

    pthread_t imu_thread;

    pthread_t ecg_thread;

    pthread_t fusion_thread;

    pthread_t logger_thread;

    pthread_t cli_thread;

    pthread_t network_thread;

    struct mq_attr attr;

    signal(

        SIGINT,

        stop_handler

    );

    signal(

        SIGTERM,

        stop_handler

    );

    /* Do not terminate the sensor process on a broken TCP connection. */

    signal(SIGPIPE, SIG_IGN);

    memset(

        &shared,

        0,

        sizeof(shared)

    );

    pthread_mutex_init(

        &shared.lock,

        NULL

    );

    i2c_fd =

        open(

            I2C_DEVICE,

            O_RDWR

        );

    if (i2c_fd < 0)

    {

        printf(

            "I2C open failed\n"

        );

        return 1;

    }

    if (configure_i2c() != 0)

    {

        printf(

            "I2C configuration failed\n"

        );

        close(i2c_fd);

        return 1;

    }

    spi_fd =

        open(

            SPI_DEVICE,

            O_RDWR

        );

    if (spi_fd < 0)

    {

        printf(

            "SPI open failed\n"

        );

        close(i2c_fd);

        return 1;

    }

    if (configure_spi() != 0)

    {

        printf(

            "SPI configuration failed\n"

        );

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (max30102_init() != 0)

    {

        printf(

            "MAX30102 initialization failed\n"

        );

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (mpu6050_init() != 0)

    {

        printf(

            "MPU6050 initialization failed\n"

        );

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (mcp3008_read(

            ECG_CHANNEL) < 0)

    {

        printf(

            "MCP3008 read failed\n"

        );

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (mpu6050_calibrate() != 0)

    {

        printf("MPU6050 calibration failed\n");

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (calibrate_ecg() != 0)

    {

        printf("ECG calibration failed\n");

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    if (max30102_calibrate() != 0)

    {

        printf("MAX30102 calibration failed\n");

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    memset(

        &attr,

        0,

        sizeof(attr)

    );

    attr.mq_maxmsg = 32;

    attr.mq_msgsize =

        sizeof(fusion_result_t);

    mq_unlink(

        QUEUE_NAME

    );

    queue_fd =

        mq_open(

            QUEUE_NAME,

            O_CREAT |

            O_RDWR |

            O_NONBLOCK,

            0666,

            &attr

        );

    if (queue_fd ==

        (mqd_t)-1)

    {

        printf(

            "Message queue creation failed\n"

        );

        close(spi_fd);

        close(i2c_fd);

        return 1;

    }

    pthread_create(

        &ppg_thread,

        NULL,

        max30102_task,

        NULL

    );

    pthread_create(

        &imu_thread,

        NULL,

        mpu6050_task,

        NULL

    );

    pthread_create(

        &ecg_thread,

        NULL,

        ecg_task,

        NULL

    );

    pthread_create(

        &fusion_thread,

        NULL,

        fusion_task,

        NULL

    );

    pthread_create(

        &logger_thread,

        NULL,

        logger_task,

        NULL

    );

    pthread_create(

        &cli_thread,

        NULL,

        cli_task,

        NULL

    );

    pthread_create(

        &network_thread,

        NULL,

        network_task,

        NULL

    );

    set_priority(

        ecg_thread,

        20

    );

    set_priority(

        ppg_thread,

        19

    );

    set_priority(

        imu_thread,

        18

    );

    set_priority(

        fusion_thread,

        17

    );

    set_priority(

        logger_thread,

        10

    );

    set_priority(

        cli_thread,

        5

    );

    set_priority(

        network_thread,

        4

    );

    while (running)

        sleep(1);

    pthread_join(

        ppg_thread,

        NULL

    );

    pthread_join(

        imu_thread,

        NULL

    );

    pthread_join(

        ecg_thread,

        NULL

    );

    pthread_join(

        fusion_thread,

        NULL

    );

    pthread_join(

        logger_thread,

        NULL

    );

    pthread_join(

        cli_thread,

        NULL

    );

    pthread_join(

        network_thread,

        NULL

    );

    mq_close(queue_fd);

    mq_unlink(QUEUE_NAME);

    close(spi_fd);

    close(i2c_fd);

    pthread_mutex_destroy(

        &shared.lock

    );

    return 0;

}


