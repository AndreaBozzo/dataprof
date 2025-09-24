# IDE Setup Guide for DataProfiler

Comprehensive setup instructions for popular IDEs and editors for DataProfiler development.

## 🚀 Quick Start

**Recommended**: Use VS Code with dev containers for the fastest setup.

```bash
# VS Code with dev containers (recommended)
code .
# Click "Reopen in Container" when prompted

# Alternative: Native VS Code setup
code dataprof.code-workspace
```

## 📋 IDE Support Matrix

| IDE | Rust Support | Debugging | Database Tools | Container Support | Recommended |
|-----|-------------|-----------|----------------|-------------------|-------------|
| **VS Code** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ **Best** |
| **CLion/RustRover** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ✅ **Excellent** |
| **Vim/Neovim** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ✅ **Advanced** |
| **Emacs** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐⭐ | ✅ **Advanced** |
| **Helix** | ⭐⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐ | ✅ **Modern** |
| **Sublime Text** | ⭐⭐⭐ | ⭐⭐ | ⭐ | ⭐ | ⚠️ Limited |

## 🎯 VS Code Setup (Recommended)

### Option 1: Dev Container Setup (Easiest)

**Prerequisites**:
- [VS Code](https://code.visualstudio.com/)
- [Remote-Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)

**Setup Steps**:
1. Clone the repository: `git clone <repo-url>`
2. Open in VS Code: `code dataprof/`
3. Click "Reopen in Container" when prompted
4. Wait for container build (5-10 minutes first time)
5. Start coding! Everything is pre-configured.

**What's Included**:
- ✅ Rust toolchain (latest stable)
- ✅ All VS Code extensions pre-installed
- ✅ Database services (PostgreSQL, MySQL, Redis)
- ✅ Development tools (just, cargo-tarpaulin, etc.)
- ✅ Pre-configured debugging
- ✅ Code formatting and linting

### Option 2: Native VS Code Setup

**Prerequisites**:
- [VS Code](https://code.visualstudio.com/)
- [Rust toolchain](https://rustup.rs/)
- [Docker](https://www.docker.com/) (for database testing)

**Setup Steps**:
1. Install required VS Code extensions:
```bash
# Install extensions via CLI
code --install-extension rust-lang.rust-analyzer
code --install-extension vadimcn.vscode-lldb
code --install-extension serayuzgur.crates
code --install-extension tamasfe.even-better-toml
code --install-extension ms-vscode-remote.remote-containers
code --install-extension ms-azuretools.vscode-docker
code --install-extension eamodio.gitlens
```

2. Open the workspace:
```bash
code dataprof.code-workspace
```

3. Install development tools:
```bash
cargo install cargo-tarpaulin cargo-machete  # Install cargo tools
```

### VS Code Extensions Explained

**Essential Rust Extensions**:
- **rust-lang.rust-analyzer**: Language server with IntelliSense, error checking, refactoring
- **vadimcn.vscode-lldb**: Native debugging support for Rust programs
- **serayuzgur.crates**: Cargo.toml dependency management and version checking

**Configuration Extensions**:
- **tamasfe.even-better-toml**: Enhanced TOML editing for Cargo.toml and config files

**Development Extensions**:
- **ms-python.python**: Python support for development scripts
- **ms-azuretools.vscode-docker**: Docker container management
- **ms-vscode-remote.remote-containers**: Dev container support

**Collaboration Extensions**:
- **eamodio.gitlens**: Enhanced Git integration with blame, history, and insights
- **github.copilot**: AI-powered code completion (optional)
- **github.copilot-chat**: AI coding assistant (optional)

**Documentation Extensions**:
- **yzhang.markdown-all-in-one**: Markdown editing and preview
- **streetsidesoftware.code-spell-checker**: Spell checking for docs and comments

### VS Code Workspace Configuration

The workspace is pre-configured with optimal settings in `.vscode/dataprof.code-workspace`:

**Key Features**:
- **Auto-formatting**: Code formats on save
- **Lint on save**: Clippy runs automatically
- **Intelligent IntelliSense**: Full Rust language support
- **Debugger integration**: One-click debugging
- **Task integration**: Quick access to `just` commands
- **Database connection forwarding**: Access dev databases from host

**Custom Tasks Available**:
- `Ctrl+Shift+P` → "Tasks: Run Task"
  - `cargo check` - Quick syntax check
  - `cargo test` - Run unit tests
  - `cargo fmt && cargo clippy && cargo test` - Full quality pipeline
  - `cargo build` - Environment setup
  - `Database Setup` - Start database services

## 🦀 JetBrains IDEs (CLion/RustRover)

### RustRover Setup (Recommended JetBrains IDE)

**Installation**:
1. Download [RustRover](https://www.jetbrains.com/rust/) (EAP available free)
2. Install Rust toolchain via rustup
3. Open project directory

**Configuration**:
```bash
# Ensure Rust toolchain is configured
rustup default stable
rustup component add rustfmt clippy rust-src

# Install development tools
cargo install cargo-tarpaulin cargo-machete
```

**Key Features**:
- ✅ Excellent debugger integration
- ✅ Built-in database tools
- ✅ Advanced refactoring
- ✅ Integrated version control
- ✅ Professional testing tools

**Recommended Settings**:
- Enable "Rust → External Linters → Clippy"
- Set "Editor → Code Style → Rust → Use rustfmt"
- Configure "Build, Execution, Deployment → Toolchain" to use local Rust
- Enable "Database Tools and SQL" plugin for database development

### CLion with Rust Plugin

**Installation**:
1. Install [CLion](https://www.jetbrains.com/clion/)
2. Install "Rust" plugin from JetBrains Marketplace
3. Configure Rust toolchain

**Setup Steps**:
```bash
# Configure Rust toolchain in CLion
Settings → Languages & Frameworks → Rust
- Toolchain location: ~/.cargo/bin/
- Standard library: (auto-detected)
```

**Database Integration**:
- Use built-in Database tools
- Configure connections to development databases
- SQL query console and schema browsing

## 📝 Vim/Neovim Setup

### Modern Neovim with LSP

**Prerequisites**:
- Neovim 0.8+ (recommended) or Vim 8.2+
- [rust-analyzer](https://rust-analyzer.github.io/)
- Node.js (for some plugins)

**Plugin Manager Setup (using packer.nvim)**:
```lua
-- ~/.config/nvim/lua/plugins.lua
return require('packer').startup(function(use)
  use 'wbthomason/packer.nvim'

  -- LSP Configuration
  use 'neovim/nvim-lspconfig'
  use 'simrat39/rust-tools.nvim'

  -- Completion
  use 'hrsh7th/nvim-cmp'
  use 'hrsh7th/cmp-nvim-lsp'
  use 'hrsh7th/cmp-buffer'
  use 'hrsh7th/cmp-path'
  use 'hrsh7th/cmp-cmdline'

  -- Snippets
  use 'L3MON4D3/LuaSnip'
  use 'saadparwaiz1/cmp_luasnip'

  -- File explorer and fuzzy finder
  use 'nvim-tree/nvim-tree.lua'
  use 'nvim-telescope/telescope.nvim'
  use 'nvim-lua/plenary.nvim'

  -- Git integration
  use 'lewis6991/gitsigns.nvim'
  use 'tpope/vim-fugitive'

  -- Database
  use 'tpope/vim-dadbod'
  use 'kristijanhusak/vim-dadbod-ui'
end)
```

**LSP Configuration**:
```lua
-- ~/.config/nvim/lua/lsp-config.lua
local lspconfig = require('lspconfig')
local rust_tools = require('rust-tools')

rust_tools.setup({
  server = {
    settings = {
      ["rust-analyzer"] = {
        checkOnSave = {
          command = "clippy"
        }
      }
    }
  }
})

-- Key mappings
local opts = { noremap=true, silent=true }
vim.api.nvim_set_keymap('n', 'gd', '<cmd>lua vim.lsp.buf.definition()<CR>', opts)
vim.api.nvim_set_keymap('n', 'K', '<cmd>lua vim.lsp.buf.hover()<CR>', opts)
vim.api.nvim_set_keymap('n', 'gi', '<cmd>lua vim.lsp.buf.implementation()<CR>', opts)
vim.api.nvim_set_keymap('n', '<leader>rn', '<cmd>lua vim.lsp.buf.rename()<CR>', opts)
```

**Development Workflow**:
```bash
# Key bindings for DataProfiler development
:lua vim.lsp.buf.format()           # Format code
:!cargo test                         # Run tests
:!cargo fmt && cargo clippy && cargo test                      # Quality checks
:!cargo run -- file.csv --quality  # Run CLI

# Telescope fuzzy finder
<leader>ff - Find files
<leader>fg - Live grep
<leader>fb - Find buffers
```

### Classic Vim Setup

**Essential Plugins** (using vim-plug):
```vim
" ~/.vimrc
call plug#begin('~/.vim/plugged')

" Rust language support
Plug 'rust-lang/rust.vim'
Plug 'dense-analysis/ale'  " Linting and LSP

" File navigation
Plug 'preservim/nerdtree'
Plug 'junegunn/fzf', { 'do': { -> fzf#install() } }
Plug 'junegunn/fzf.vim'

" Git integration
Plug 'tpope/vim-fugitive'
Plug 'airblade/vim-gitgutter'

" Status line
Plug 'vim-airline/vim-airline'

call plug#end()

" Rust configuration
let g:rustfmt_autosave = 1
let g:ale_rust_cargo_use_check = 1
let g:ale_rust_cargo_check_tests = 1
```

## 📄 Emacs Setup

### Doom Emacs Configuration

**Installation**:
```bash
# Install Doom Emacs
git clone --depth 1 https://github.com/doomemacs/doomemacs ~/.emacs.d
~/.emacs.d/bin/doom install
```

**Rust Configuration** (`~/.doom.d/init.el`):
```elisp
:lang
(rust +lsp)        ; Fe2O3.unwrap().unwrap().unwrap().unwrap()

:tools
lsp                ; Language Server Protocol
magit              ; Git porcelain
docker             ; Container management
```

**Custom Configuration** (`~/.doom.d/config.el`):
```elisp
;; Rust development configuration
(setq rust-format-on-save t)
(setq lsp-rust-analyzer-cargo-watch-command "clippy")
(setq lsp-rust-analyzer-proc-macro-enable t)

;; DataProfiler specific bindings
(map! :leader
      (:prefix-map ("d" . "dataprof")
       :desc "Run tests" "t" (cmd! (compile "cargo test"))
       :desc "Quality check" "q" (cmd! (compile "cargo fmt && cargo clippy && cargo test"))
       :desc "Setup environment" "s" (cmd! (compile "cargo build"))))
```

### Spacemacs Configuration

**Installation**:
```bash
git clone https://github.com/syl20bnr/spacemacs ~/.emacs.d
```

**Configuration** (`~/.spacemacs`):
```elisp
;; Configuration layers
dotspacemacs-configuration-layers
'(
  ;; Language support
  rust

  ;; Development tools
  git
  lsp
  syntax-checking

  ;; Version control
  github

  ;; Database
  sql
  )

;; Rust layer configuration
(rust :variables
      rust-format-on-save t
      rust-backend 'lsp)
```

## ⚡ Helix Editor Setup

**Installation**:
```bash
# Install Helix
cargo install helix-term

# Or via package manager (varies by OS)
```

**Configuration** (`~/.config/helix/config.toml`):
```toml
theme = "onedark"

[editor]
line-number = "relative"
mouse = true
completion-trigger-len = 2
auto-format = true
auto-save = true

[editor.cursor-shape]
insert = "bar"
normal = "block"
select = "underline"

[editor.file-picker]
hidden = false

[keys.normal]
# DataProfiler specific bindings
"<leader>t" = ":sh cargo test"
"<leader>q" = ":sh cargo fmt && cargo clippy && cargo test"
"<leader>r" = ":sh cargo run"
```

**Language Server Configuration** (`~/.config/helix/languages.toml`):
```toml
[[language]]
name = "rust"
auto-format = true
formatter = { command = "rustfmt", args = ["--edition", "2021"] }

[language.config.checkOnSave]
command = "clippy"
```

## 🛠️ Universal Development Setup

### Essential Tools (All IDEs)

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install essential components
rustup component add rustfmt clippy rust-src rust-analyzer

# Install development tools
cargo install cargo-tarpaulin cargo-machete cargo-outdated

# Install database clients (optional)
# PostgreSQL client
sudo apt-get install postgresql-client  # Ubuntu/Debian
brew install postgresql                 # macOS

# MySQL client
sudo apt-get install mysql-client       # Ubuntu/Debian
brew install mysql-client              # macOS
```

### Environment Configuration

**Bash/Zsh Configuration** (`~/.bashrc` or `~/.zshrc`):
```bash
# Rust environment
export PATH="$HOME/.cargo/bin:$PATH"
export RUST_BACKTRACE=1

# DataProfiler development aliases
alias dp-test='cargo test'
alias dp-quality='cargo fmt && cargo clippy && cargo test'
alias dp-setup='cargo build'
alias dp-db='docker-compose -f .devcontainer/docker-compose.yml up -d'

# Useful environment variables
export RUST_LOG=debug
export DATAPROF_DEV=1
```

**Fish Shell Configuration** (`~/.config/fish/config.fish`):
```fish
# Rust environment
set -gx PATH $HOME/.cargo/bin $PATH
set -gx RUST_BACKTRACE 1

# DataProfiler aliases
alias dp-test='cargo test'
alias dp-quality='cargo fmt && cargo clippy && cargo test'
alias dp-setup='cargo build'
```

## 🐳 Container Development

### VS Code Dev Containers (Recommended)

The project includes a complete dev container setup in `.devcontainer/`:

**Features**:
- ✅ Pre-configured Rust environment
- ✅ Database services (PostgreSQL, MySQL, Redis)
- ✅ All development tools installed
- ✅ VS Code extensions pre-installed
- ✅ Port forwarding for database access
- ✅ Volume mounts for performance

**Usage**:
```bash
# Open in dev container
code .
# Click "Reopen in Container"

# Or use CLI
code --folder-uri vscode-remote://dev-container+$(pwd | sed 's/ /%20/g')
```

### Docker Development Environment

**Manual Docker Setup**:
```bash
# Build development image
docker build -f .devcontainer/Dockerfile -t dataprof-dev .

# Run development container
docker run -it --rm \
  -v $(pwd):/workspace \
  -v dataprof-cargo:/usr/local/cargo \
  -p 5432:5432 -p 3306:3306 \
  dataprof-dev
```

### GitHub Codespaces

**One-Click Cloud Development**:
1. Fork the repository on GitHub
2. Click "Code" → "Create codespace on main"
3. Wait for environment setup (5-10 minutes)
4. Start developing in the browser!

**Codespace Features**:
- ✅ Full VS Code in browser
- ✅ Pre-configured development environment
- ✅ Database services available
- ✅ Git integration
- ✅ Terminal access

## 🔧 Debugging Configuration

### VS Code Debugging

**Pre-configured Debug Configurations**:
- **Debug unit tests**: Debug specific test cases
- **Debug CLI**: Debug CLI application with sample data
- **Debug with custom arguments**: Customize arguments in launch.json

**Usage**:
1. Set breakpoints in code
2. Press `F5` or use "Run and Debug" panel
3. Select appropriate configuration
4. Debug with full variable inspection

### CLion/RustRover Debugging

**Setup**:
1. Build → Edit Configurations
2. Add "Cargo Command" configuration
3. Configure command: `run --bin dataprof-cli`
4. Set program arguments: `sample.csv --quality`

### Terminal Debugging

**GDB/LLDB**:
```bash
# Build with debug symbols
cargo build

# Debug with gdb
gdb ./target/debug/dataprof-cli
(gdb) run sample.csv --quality
(gdb) bt  # Backtrace on crash

# Debug with lldb (macOS)
lldb ./target/debug/dataprof-cli
(lldb) run sample.csv --quality
(lldb) bt  # Backtrace on crash
```

## 📊 Database IDE Integration

### DataGrip (JetBrains)

**Setup for DataProfiler Development**:
1. Install [DataGrip](https://www.jetbrains.com/datagrip/)
2. Connect to development databases:
   - PostgreSQL: `localhost:5432`, user: `dataprof`, password: `dev_password_123`
   - MySQL: `localhost:3306`, user: `dataprof`, password: `dev_password_123`

### DBeaver (Free Alternative)

**Setup**:
1. Install [DBeaver Community](https://dbeaver.io/download/)
2. Create connections to development databases
3. Use for database schema exploration and query testing

### VS Code Database Extensions

**Recommended Extensions**:
- **ms-mssql.mssql**: SQL Server support
- **mtxr.sqltools**: Multi-database SQL tools
- **cweijan.vscode-database-client2**: Database client with GUI

## 🔍 Code Quality Integration

### Pre-commit Hooks

**Setup** (works with all editors):
```bash
pip install pre-commit
pre-commit install

# Manual run
pre-commit run --all-files
```

**Hook Configuration** (`.pre-commit-config.yaml`):
```yaml
repos:
  - repo: local
    hooks:
      - id: cargo-fmt
        name: cargo fmt
        entry: cargo fmt
        language: system
        types: [rust]

      - id: cargo-clippy
        name: cargo clippy
        entry: cargo clippy -- -D warnings
        language: system
        types: [rust]
```

### Editor Integration

**Format on Save** (all editors):
- VS Code: Configured in workspace settings
- CLion/RustRover: Settings → Rust → Use rustfmt
- Vim/Neovim: `:lua vim.lsp.buf.format()`
- Emacs: `(rust-format-buffer)`
- Helix: Auto-format enabled in config

## 🚀 Quick IDE Comparison

### For Beginners
**Recommended**: VS Code with dev containers
- Easy setup with one-click container development
- Excellent Rust support out of the box
- Great debugging experience
- Free and lightweight

### For Professional Development
**Recommended**: CLion/RustRover
- Advanced debugging capabilities
- Excellent refactoring tools
- Built-in database tools
- Professional code analysis

### For Terminal Enthusiasts
**Recommended**: Neovim with LSP
- Lightweight and fast
- Highly customizable
- Keyboard-driven workflow
- Modern LSP integration

### For Emacs Users
**Recommended**: Doom Emacs with Rust layer
- Powerful editing capabilities
- Excellent Git integration
- Extensible configuration
- Strong community support

## 📚 Additional Resources

- [Rust IDE Comparison](https://areweideyet.com/)
- [rust-analyzer User Manual](https://rust-analyzer.github.io/manual.html)
- [VS Code Rust Extension Guide](https://code.visualstudio.com/docs/languages/rust)
- [DataProfiler Development Guide](./DEVELOPMENT.md)
- [Testing Guide](./TESTING.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
