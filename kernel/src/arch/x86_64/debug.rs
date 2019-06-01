use core::ops::Deref;
use x86::io;

//use alloc::boxed::Box;

use super::irq;
use super::ExitReason;

use super::process::CURRENT_PROCESS;

static PORT0: u16 = 0x3f8; /* COM1 */
static PORT2: u16 = 0x2F8; /* COM1 */

//static COM1_IRQ: usize = 4 + 32;
//static COM1_IRQ: usize = 5 + 32; // XXX

pub fn init() {
    unsafe {
        io::outb(PORT0 + 1, 0x00); // Disable all interrupts
        io::outb(PORT0 + 3, 0x80); // Enable DLAB (set baud rate divisor)
        io::outb(PORT0 + 0, 0x01); // Set divisor to 1 (lo byte) 115200 baud
        io::outb(PORT0 + 1, 0x00); //                  (hi byte)
        io::outb(PORT0 + 3, 0x03); // 8 bits, no parity, one stop bit
        io::outb(PORT0 + 2, 0xC7); // Enable FIFO, clear them, with 14-byte threshold
        io::outb(PORT0 + 1, 0x01); // Enable receive data IRQ
                                   //io::outb(PORT0 + 1, 0x00);    // Disable receive data IRQ

        io::outb(PORT2 + 1, 0x00); // Disable all interrupts
        io::outb(PORT2 + 3, 0x80); // Enable DLAB (set baud rate divisor)
        io::outb(PORT2 + 0, 0x01); // Set divisor to 1 (lo byte) 115200 baud
        io::outb(PORT2 + 1, 0x00); //                  (hi byte)
        io::outb(PORT2 + 3, 0x03); // 8 bits, no parity, one stop bit
        io::outb(PORT2 + 2, 0xC7); // Enable FIFO, clear them, with 14-byte threshold
        io::outb(PORT2 + 1, 0x01); // Enable receive data IRQ
                                   //io::outb(PORT0 + 1, 0x00);    // Disable receive data IRQ
    }
    debug!("serial initialized");
    /*unsafe {
        irq::register_handler(COM1_IRQ, Box::new(|e| receive_serial_irq(e)));
    }*/
}

#[allow(unused)]
unsafe fn receive_serial_irq(_a: &irq::ExceptionArguments) {
    let scancode = io::inb(PORT0 + 0);
    let cp = CURRENT_PROCESS.lock();
    match *cp.deref() {
        Some(ref p) => {
            debug!("p = {:?}", p);
            putb(scancode);
            panic!("p.resume();");
        }
        None => debug!("No process"),
    };
    //loop {}
}

/// Write a string to the output channel
pub unsafe fn puts(s: &str) {
    for b in s.bytes() {
        putb(b);
    }
}

/// Write a single byte to the output channel
pub unsafe fn putb(b: u8) {
    // Wait for the serial PORT0's FIFO to be ready
    while (io::inb(PORT0 + 5) & 0x20) == 0 {}
    // Send the byte out the serial PORT0
    io::outb(PORT0, b);

    // Wait for the serial PORT1's FIFO to be ready
    while (io::inb(PORT2 + 5) & 0x20) == 0 {}
    // Send the byte out the serial PORT2
    io::outb(PORT2, b);
}

/// Shutdown the processor.
///
/// Currently we only support the debug exit method from qemu, which conveniently
/// allows us to supply an exit code for testing purposes.
pub fn shutdown(val: ExitReason) -> ! {
    // Ok for QEMU with debug-exit,iobase=0xf4,iosize=0x04
    // qemu will call: exit((val << 1) | 1);
    unsafe {
        io::outb(0xf4, val as u8);
    }
    // In case this doesn't work we hang.
    loop {}
}
