import { execFileSync } from "node:child_process"
import process from "node:process"

const exe = execFileSync("effect-tsgo", ["get-exe-path"], {
  encoding: "utf8",
  shell: true
}).trim()

try {
  execFileSync(exe, ["-p", "tsconfig.json", "--noEmit"], { stdio: "inherit", shell: true })
} catch {
  process.exit(1)
}
