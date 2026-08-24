// Injected with NODE_OPTIONS=--require. Makes every TCP listen() in the process
// set SO_REUSEPORT, without the application knowing.
//
// This exists because the frameworks Depfloy runs — react-router-serve, next
// start, nuxt's nitro server — call listen(port) themselves and expose no way to
// pass options through. Patching the one place Node funnels all of them through
// is less invasive than forking each serve binary.
const net = require('net');

const original = net.Server.prototype.listen;

net.Server.prototype.listen = function patchedListen(...args) {
    // listen(options[, callback]) — the shape that already carries options.
    if (args.length > 0 && typeof args[0] === 'object' && args[0] !== null && !Array.isArray(args[0])) {
        const opts = args[0];
        // Only TCP ports. A unix socket path or a file descriptor has no
        // SO_REUSEPORT, and setting it makes Node throw on those platforms.
        if (opts.port !== undefined && opts.path === undefined && opts.fd === undefined && opts.reusePort === undefined) {
            args[0] = { ...opts, reusePort: true };
        }
        return original.apply(this, args);
    }

    // listen(port[, host][, backlog][, callback]) — the shape frameworks use.
    // Rebuild it as an options object so reusePort can ride along, preserving
    // whatever positional arguments were actually supplied.
    if (args.length > 0 && (typeof args[0] === 'number' || (typeof args[0] === 'string' && /^\d+$/.test(args[0])))) {
        const opts = { port: Number(args[0]), reusePort: true };
        const rest = [];
        for (const arg of args.slice(1)) {
            if (typeof arg === 'string') opts.host = arg;
            else if (typeof arg === 'number') opts.backlog = arg;
            else if (typeof arg === 'function') rest.push(arg);
        }
        return original.call(this, opts, ...rest);
    }

    return original.apply(this, args);
};

if (process.env.REUSEPORT_SHIM_VERBOSE === '1') {
    console.log(`EVENT shim_loaded pid=${process.pid}`);
}
